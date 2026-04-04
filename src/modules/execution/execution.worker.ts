import { randomUUID } from 'node:crypto';
import config from '../../config/env.js';
import executionRepository from './execution.repository.js';
import executionService from './execution.service.js';
import logger from '../../utils/logger.js';

export class ExecutionWorker {
  private readonly lockOwner = `executor-${randomUUID()}`;
  private timer: NodeJS.Timeout | null = null;
  private inProgress = false;

  start() {
    if (this.timer) return;

    logger.info('Execution worker started', {
      lockOwner: this.lockOwner,
      pollIntervalMs: config.workerPollIntervalMs,
      batchSize: config.workerBatchSize,
    });

    this.timer = setInterval(() => {
      this.tick().catch((error) => {
        logger.error('Worker tick failed', { error: error?.message || 'Unknown tick failure' });
      });
    }, config.workerPollIntervalMs);

    void this.tick();
  }

  async stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async tick() {
    if (this.inProgress) return;
    this.inProgress = true;

    try {
      const jobs = await executionRepository.claimAvailableJobs({
        limit: config.workerBatchSize,
        lockOwner: this.lockOwner,
        lockTimeoutMs: config.queueLockTimeoutMs,
      });

      if (jobs.length === 0) return;

      for (const job of jobs) {
        try {
          await executionService.processJob(job);
        } catch (error: any) {
          logger.error('Failed processing queue job', {
            queueJobId: job.id,
            executionId: job.executionId,
            error: error?.message || 'Unknown processing error',
          });

          await executionRepository.finalizeExecution({
            executionId: job.executionId,
            queueJobId: job.id,
            status: 'failed',
            errorSummary: error?.message || 'Unhandled job processing error',
          });
        }
      }
    } finally {
      this.inProgress = false;
    }
  }
}

export default new ExecutionWorker();
