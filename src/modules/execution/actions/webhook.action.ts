import { StepExecutionContext, StepExecutionResult } from '../execution.types.js';
import { executeHttpAction } from './http.action.js';

type WebhookActionConfig = {
  url: string;
  method?: string;
  headers?: Record<string, string>;
  payload?: Record<string, unknown>;
  timeoutMs?: number;
  successStatusCodes?: number[];
};

export async function executeWebhookAction(config: WebhookActionConfig, context: StepExecutionContext): Promise<StepExecutionResult> {
  return executeHttpAction(
    {
      method: config.method || 'POST',
      url: config.url,
      headers: config.headers,
      body: config.payload,
      timeoutMs: config.timeoutMs,
      successStatusCodes: config.successStatusCodes,
    },
    context,
  );
}
