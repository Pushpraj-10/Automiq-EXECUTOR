import { StepExecutionContext, StepExecutionResult } from '../execution.types.js';

type DelayConfig = {
  durationMs: number;
};

export async function executeDelayAction(config: DelayConfig, _context: StepExecutionContext): Promise<StepExecutionResult> {
  const durationMs = Number(config.durationMs || 0);
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    return { success: false, error: 'delay.durationMs must be a positive number' };
  }

  await new Promise<void>((resolve) => setTimeout(resolve, durationMs));
  return {
    success: true,
    output: { delayedForMs: durationMs },
  };
}
