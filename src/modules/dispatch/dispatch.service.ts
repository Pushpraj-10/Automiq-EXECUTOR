import { executeDelayAction } from '../execution/actions/delay.action.js';
import { executeEmailAction } from '../execution/actions/email.action.js';
import { executeHttpAction } from '../execution/actions/http.action.js';
import { executeWebhookAction } from '../execution/actions/webhook.action.js';
import { StepExecutionContext, StepExecutionResult } from '../execution/execution.types.js';
import { reportExecutionStatus, reportExecutionStepStatus } from './dispatcher.grpc.client.js';
import logger from '../../utils/logger.js';

type WorkflowActionType = 'http_request' | 'send_email' | 'webhook_notification' | 'delay';

type DispatchStep = {
  id: string;
  stepIndex: number;
  type: WorkflowActionType;
  name?: string;
  config: Record<string, unknown>;
  onFailure?: {
    strategy?: 'retry' | 'stop' | 'continue';
    maxAttempts?: number;
  };
};

type DispatchExecutionInput = {
  executionId: string;
  tenantId: string;
  steps: DispatchStep[];
};

const RETRY_BACKOFF_MS = [30_000, 120_000, 600_000];
const DEFAULT_MAX_ATTEMPTS = 3;

export class DispatchService {
  private readonly inFlightExecutions = new Set<string>();

  enqueueExecution(input: DispatchExecutionInput) {
    if (this.inFlightExecutions.has(input.executionId)) return;

    this.inFlightExecutions.add(input.executionId);
    void this.runExecution(input)
      .catch((error: any) => {
        logger.error('Dispatch execution failed unexpectedly', {
          executionId: input.executionId,
          error: error?.message || 'Unknown error',
        });
      })
      .finally(() => {
        this.inFlightExecutions.delete(input.executionId);
      });
  }

  private async runExecution(input: DispatchExecutionInput) {
    await reportExecutionStatus({
      executionId: input.executionId,
      status: 'running',
    });

    const context: StepExecutionContext = {
      tenantId: input.tenantId,
      executionId: input.executionId,
    };

    const sortedSteps = [...input.steps].sort((a, b) => a.stepIndex - b.stepIndex);

    for (const step of sortedSteps) {
      const maxAttempts = Math.max(1, Math.min(step.onFailure?.maxAttempts || DEFAULT_MAX_ATTEMPTS, 10));
      const strategy = step.onFailure?.strategy || 'stop';

      let lastResult: StepExecutionResult = { success: false, error: 'Step was not executed' };

      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        await reportExecutionStepStatus({
          executionId: input.executionId,
          stepIndex: step.stepIndex,
          stepType: step.type,
          status: 'running',
          attemptCount: attempt,
          requestJson: step,
        });

        lastResult = await this.executeStep(step, context);

        await reportExecutionStepStatus({
          executionId: input.executionId,
          stepIndex: step.stepIndex,
          stepType: step.type,
          status: lastResult.success ? 'succeeded' : 'failed',
          attemptCount: attempt,
          requestJson: step,
          responseJson: lastResult.output,
          errorMessage: lastResult.error,
        });

        if (lastResult.success) break;
        if (attempt < maxAttempts) {
          const backoffMs = RETRY_BACKOFF_MS[Math.min(attempt - 1, RETRY_BACKOFF_MS.length - 1)];
          await new Promise<void>((resolve) => setTimeout(resolve, backoffMs));
        }
      }

      if (!lastResult.success && strategy !== 'continue') {
        await reportExecutionStatus({
          executionId: input.executionId,
          status: 'failed',
          errorSummary: lastResult.error || `Step ${step.stepIndex} failed`,
        });
        return;
      }
    }

    await reportExecutionStatus({
      executionId: input.executionId,
      status: 'succeeded',
    });
  }

  private async executeStep(step: DispatchStep, context: StepExecutionContext): Promise<StepExecutionResult> {
    try {
      if (step.type === 'http_request') {
        return executeHttpAction(step.config as any, context);
      }

      if (step.type === 'send_email') {
        return executeEmailAction(step.config as any, context);
      }

      if (step.type === 'webhook_notification') {
        return executeWebhookAction(step.config as any, context);
      }

      if (step.type === 'delay') {
        return executeDelayAction(step.config as any, context);
      }

      return { success: false, error: `Unsupported action type: ${step.type}` };
    } catch (error: any) {
      return { success: false, error: error?.message || 'Unhandled execution error' };
    }
  }
}

export default new DispatchService();
