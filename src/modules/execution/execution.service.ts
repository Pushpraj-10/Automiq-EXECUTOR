import executionRepository from './execution.repository.js';
import { executeDelayAction } from './actions/delay.action.js';
import { executeEmailAction } from './actions/email.action.js';
import { executeHttpAction } from './actions/http.action.js';
import { executeWebhookAction } from './actions/webhook.action.js';
import { QueueJob, StepExecutionContext, StepExecutionResult, WorkflowStep } from './execution.types.js';
import logger from '../../utils/logger.js';
import { reportExecutionStatus, reportExecutionStepStatus } from '../dispatch/dispatcher.grpc.client.js';

const RETRY_BACKOFF_MS = [30_000, 120_000, 600_000];
const DEFAULT_MAX_ATTEMPTS = 3;

export class ExecutionService {
  constructor(private readonly repo = executionRepository) {}

  async processJob(job: QueueJob): Promise<void> {
    const execution = await this.repo.getExecutionWithWorkflowDefinition(job.executionId);
    if (!execution) {
      await this.repo.finalizeExecution({
        executionId: job.executionId,
        queueJobId: job.id,
        status: 'failed',
        errorSummary: 'Execution not found',
      });
      return;
    }

    await this.repo.markExecutionRunning(execution.id);
    await this.safeReportExecutionStatus({
      correlationId: execution.correlationId,
      executionId: execution.id,
      status: 'running',
    });

    const context: StepExecutionContext = {
      tenantId: execution.tenantId,
      executionId: execution.id,
    };

    for (let i = 0; i < execution.workflowVersionDefinition.steps.length; i += 1) {
      const step = execution.workflowVersionDefinition.steps[i] as WorkflowStep;
      const stepIndex = Number((step as any).stepIndex || i + 1);
      const maxAttempts = Math.max(1, Math.min(step.onFailure?.maxAttempts || DEFAULT_MAX_ATTEMPTS, 10));
      const strategy = step.onFailure?.strategy || 'stop';

      let lastResult: StepExecutionResult = { success: false, error: 'Step was not executed' };

      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        await this.safeReportStepStatus({
          correlationId: execution.correlationId,
          executionId: execution.id,
          stepIndex,
          stepType: step.type,
          status: 'running',
          attemptCount: attempt,
          requestJson: step,
        });

        lastResult = await this.executeStep(step, context);

        await this.repo.insertStepLog({
          executionId: execution.id,
          stepIndex,
          stepType: step.type,
          status: lastResult.success ? 'succeeded' : 'failed',
          attemptCount: attempt,
          requestJson: step,
          responseJson: lastResult.output,
          errorMessage: lastResult.error,
        });

        await this.safeReportStepStatus({
          correlationId: execution.correlationId,
          executionId: execution.id,
          stepIndex,
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

      if (!lastResult.success) {
        if (strategy === 'continue') {
          logger.warn('Step failed and continued due to onFailure strategy', {
            executionId: execution.id,
            stepIndex,
            stepType: step.type,
            error: lastResult.error || undefined,
          });
          continue;
        }

        await this.repo.finalizeExecution({
          executionId: execution.id,
          queueJobId: job.id,
          status: 'failed',
          errorSummary: lastResult.error || `Step ${i + 1} failed`,
        });
        await this.safeReportExecutionStatus({
          correlationId: execution.correlationId,
          executionId: execution.id,
          status: 'failed',
          errorSummary: lastResult.error || `Step ${stepIndex} failed`,
        });
        return;
      }
    }

    await this.repo.finalizeExecution({
      executionId: execution.id,
      queueJobId: job.id,
      status: 'succeeded',
    });
    await this.safeReportExecutionStatus({
      correlationId: execution.correlationId,
      executionId: execution.id,
      status: 'succeeded',
    });
  }

  private async safeReportExecutionStatus(input: {
    correlationId?: string;
    executionId: string;
    status: 'queued' | 'running' | 'succeeded' | 'failed';
    errorSummary?: string;
  }) {
    try {
      await reportExecutionStatus(input);
    } catch (error: any) {
      logger.warn('Failed to report execution status to backend', {
        executionId: input.executionId,
        status: input.status,
        error: error?.message || 'Unknown gRPC status report error',
      });
    }
  }

  private async safeReportStepStatus(input: {
    correlationId?: string;
    executionId: string;
    stepIndex: number;
    status: 'queued' | 'running' | 'succeeded' | 'failed';
    stepType: string;
    attemptCount: number;
    requestJson?: unknown;
    responseJson?: unknown;
    errorMessage?: string;
  }) {
    try {
      await reportExecutionStepStatus(input);
    } catch (error: any) {
      logger.warn('Failed to report execution step status to backend', {
        executionId: input.executionId,
        stepIndex: input.stepIndex,
        status: input.status,
        error: error?.message || 'Unknown gRPC step report error',
      });
    }
  }

  private async executeStep(step: WorkflowStep, context: StepExecutionContext): Promise<StepExecutionResult> {
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
      logger.error('Step execution threw error', {
        type: step.type,
        error: error?.message || 'Unknown error',
      });
      return { success: false, error: error?.message || 'Unhandled execution error' };
    }
  }
}

export default new ExecutionService();
