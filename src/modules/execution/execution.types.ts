export type WorkflowActionType = 'http_request' | 'send_email' | 'webhook_notification' | 'delay';

export type QueueJobStatus = 'queued' | 'processing' | 'completed' | 'failed';

export type QueueJob = {
  id: string;
  executionId: string;
  tenantId: string;
  status: QueueJobStatus;
  retryCount: number;
  availableAt: string;
};

export type ExecutionRecord = {
  id: string;
  tenantId: string;
  correlationId?: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed';
  attemptCount: number;
  workflowVersionDefinition: {
    steps: WorkflowStep[];
  };
};

export type WorkflowStep = {
  id: string;
  type: WorkflowActionType;
  name?: string;
  config: Record<string, unknown>;
  onFailure?: {
    strategy?: 'retry' | 'stop' | 'continue';
    maxAttempts?: number;
  };
};

export type StepExecutionContext = {
  tenantId: string;
  executionId: string;
};

export type StepExecutionResult = {
  success: boolean;
  output?: unknown;
  error?: string;
  statusCode?: number;
};
