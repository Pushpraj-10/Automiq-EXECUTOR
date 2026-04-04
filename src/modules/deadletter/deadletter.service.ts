import executionRepository, { DeadLetterRecord } from '../execution/execution.repository.js';

export class DeadLetterService {
  constructor(private readonly repo = executionRepository) {}

  async listDeadLetters(limit = 100): Promise<DeadLetterRecord[]> {
    return this.repo.listDeadLetters(limit);
  }

  async replayDeadLetter(deadLetterId: string): Promise<DeadLetterRecord | null> {
    if (!deadLetterId) throw new Error('deadLetterId is required');
    return this.repo.replayDeadLetter(deadLetterId);
  }
}

export default new DeadLetterService();
