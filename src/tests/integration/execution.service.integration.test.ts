import { describe, expect, it, vi } from 'vitest';

vi.mock('../../modules/dispatch/dispatcher.grpc.client.js', () => ({
  reportExecutionStatus: vi.fn().mockResolvedValue(undefined),
  reportExecutionStepStatus: vi.fn().mockResolvedValue(undefined),
}));

vi.mock('../../modules/execution/execution.repository.js', () => ({
  default: {},
}));

import { reportExecutionStatus, reportExecutionStepStatus } from '../../modules/dispatch/dispatcher.grpc.client.js';
import { ExecutionService } from '../../modules/execution/execution.service.js';

describe('ExecutionService integration behavior', () => {
  it('retries failed step and reports failed execution', async () => {
    const timeoutSpy = vi
      .spyOn(global, 'setTimeout')
      .mockImplementation(((handler: any) => {
        if (typeof handler === 'function') handler();
        return 0 as any;
      }) as any);

    const repo = {
      getExecutionWithWorkflowDefinition: vi.fn().mockResolvedValue({
        id: 'exec_1',
        tenantId: 'tenant_1',
        correlationId: 'corr_1',
        status: 'queued',
        attemptCount: 0,
        workflowVersionDefinition: {
          steps: [
            {
              id: 'step_1',
              stepIndex: 1,
              type: 'delay',
              config: { durationMs: 0 },
              onFailure: { maxAttempts: 3 },
            },
          ],
        },
      }),
      markExecutionRunning: vi.fn().mockResolvedValue(undefined),
      insertStepLog: vi.fn().mockResolvedValue(undefined),
      finalizeExecution: vi.fn().mockResolvedValue(undefined),
    } as any;

    const service = new ExecutionService(repo);

    await service.processJob({
      id: 'job_1',
      executionId: 'exec_1',
      tenantId: 'tenant_1',
      status: 'processing',
      retryCount: 0,
      availableAt: new Date().toISOString(),
    });

    expect(repo.insertStepLog).toHaveBeenCalledTimes(3);
    expect(repo.finalizeExecution).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'failed' }),
    );

    expect(reportExecutionStatus).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'running' }),
    );
    expect(reportExecutionStatus).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'failed' }),
    );

    expect(reportExecutionStepStatus).toHaveBeenCalled();

    timeoutSpy.mockRestore();
  });

  it('completes successful execution and reports success', async () => {
    const repo = {
      getExecutionWithWorkflowDefinition: vi.fn().mockResolvedValue({
        id: 'exec_2',
        tenantId: 'tenant_2',
        correlationId: 'corr_2',
        status: 'queued',
        attemptCount: 0,
        workflowVersionDefinition: {
          steps: [
            {
              id: 'step_1',
              stepIndex: 1,
              type: 'delay',
              config: { durationMs: 1 },
            },
          ],
        },
      }),
      markExecutionRunning: vi.fn().mockResolvedValue(undefined),
      insertStepLog: vi.fn().mockResolvedValue(undefined),
      finalizeExecution: vi.fn().mockResolvedValue(undefined),
    } as any;

    const service = new ExecutionService(repo);

    await service.processJob({
      id: 'job_2',
      executionId: 'exec_2',
      tenantId: 'tenant_2',
      status: 'processing',
      retryCount: 0,
      availableAt: new Date().toISOString(),
    });

    expect(repo.finalizeExecution).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'succeeded' }),
    );

    expect(reportExecutionStatus).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'succeeded' }),
    );
  });
});
