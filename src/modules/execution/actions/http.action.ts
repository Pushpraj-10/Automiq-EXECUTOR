import { StepExecutionContext, StepExecutionResult } from '../execution.types.js';

type HttpActionConfig = {
  method?: string;
  url: string;
  headers?: Record<string, string>;
  query?: Record<string, string | number | boolean>;
  body?: unknown;
  timeoutMs?: number;
  successStatusCodes?: number[];
};

export async function executeHttpAction(config: HttpActionConfig, _context: StepExecutionContext): Promise<StepExecutionResult> {
  if (!config.url) {
    return { success: false, error: 'http_request config.url is required' };
  }

  const method = (config.method || 'POST').toUpperCase();
  const url = new URL(config.url);

  if (config.query) {
    for (const [k, v] of Object.entries(config.query)) {
      url.searchParams.set(k, String(v));
    }
  }

  const headers = { ...(config.headers || {}) };
  let body: string | undefined;

  if (config.body !== undefined && !['GET', 'HEAD'].includes(method)) {
    if (!headers['Content-Type']) headers['Content-Type'] = 'application/json';
    body = headers['Content-Type'].includes('application/json') ? JSON.stringify(config.body) : String(config.body);
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.timeoutMs ?? 10000);

  try {
    const response = await fetch(url.toString(), {
      method,
      headers,
      body,
      signal: controller.signal,
    });

    const contentType = response.headers.get('content-type') || '';
    const output = contentType.includes('application/json')
      ? await response.json().catch(() => null)
      : await response.text().catch(() => '');

    const success = config.successStatusCodes
      ? config.successStatusCodes.includes(response.status)
      : response.status >= 200 && response.status < 300;

    return {
      success,
      statusCode: response.status,
      output,
      error: success ? undefined : `Unexpected status code ${response.status}`,
    };
  } catch (error: any) {
    return {
      success: false,
      error: error?.message || 'HTTP action failed',
    };
  } finally {
    clearTimeout(timeout);
  }
}
