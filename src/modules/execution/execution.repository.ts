import { randomUUID } from 'node:crypto';
import { Filter, ObjectId } from 'mongodb';
import { getCollection } from '../../config/db.js';
import { ExecutionRecord, QueueJob, QueueJobStatus } from './execution.types.js';
import { QueuedAction } from '../../models/action.js';

type ExecutionQueueDoc = {
  _id?: ObjectId | string;
  id?: string;
  executionId: string;
  tenantId: string;
  status: QueueJobStatus;
  retryCount?: number;
  availableAt: Date | string;
  lockedAt?: Date | null;
  lockOwner?: string | null;
  createdAt?: Date;
  updatedAt?: Date;
};

type ExecutionDoc = {
  _id?: ObjectId | string;
  id?: string;
  tenantId: string;
  correlationId?: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed';
  attemptCount?: number;
  workflowVersionId?: string | ObjectId;
};

type WorkflowVersionDoc = {
  _id?: ObjectId | string;
  id?: string;
  definitionJson?: {
    steps?: unknown[];
  };
};

type ExecutionStepDoc = {
  _id?: ObjectId | string;
  id?: string;
  executionId: string;
  stepIndex: number;
  stepType: string;
  status: string;
  attemptCount: number;
  requestJson?: Record<string, unknown>;
  responseJson?: Record<string, unknown>;
  errorMessage?: string | null;
  startedAt?: Date | null;
  finishedAt?: Date | null;
  updatedAt?: Date | null;
};

type ExecutionDeadLetterDoc = {
  _id?: ObjectId | string;
  id?: string;
  executionId: string;
  queueJobId: string;
  tenantId?: string;
  correlationId?: string;
  lastErrorSummary?: string;
  status: 'active' | 'replayed' | 'resolved';
  firstFailedAt: Date;
  lastFailedAt: Date;
  replayCount: number;
  lastReplayAt?: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type DeadLetterRecord = {
  id: string;
  executionId: string;
  queueJobId: string;
  tenantId?: string;
  correlationId?: string;
  lastErrorSummary?: string;
  status: 'active' | 'replayed' | 'resolved';
  firstFailedAt: string;
  lastFailedAt: string;
  replayCount: number;
  lastReplayAt?: string;
  createdAt: string;
  updatedAt: string;
};

export class ExecutionRepository {
  async enqueueDispatchedExecution(input: {
    executionId: string;
    tenantId: string;
    workflowId: string;
    workflowVersionId: string;
    correlationId?: string;
    steps: QueuedAction[];
  }): Promise<void> {
    const executionCollection = await getCollection<ExecutionDoc>('Execution');
    const workflowVersionCollection = await getCollection<WorkflowVersionDoc>('WorkflowVersion');
    const queueCollection = await getCollection<ExecutionQueueDoc>('ExecutionQueue');
    const stepCollection = await getCollection('ExecutionStep');
    const now = new Date();

    await workflowVersionCollection.updateOne(
      this.idFilter<WorkflowVersionDoc>(input.workflowVersionId),
      {
        $setOnInsert: {
          id: input.workflowVersionId,
          createdAt: now,
        },
        $set: {
          definitionJson: { steps: input.steps },
          updatedAt: now,
        },
      },
      { upsert: true },
    );

    await executionCollection.updateOne(
      this.idFilter<ExecutionDoc>(input.executionId),
      {
        $setOnInsert: {
          id: input.executionId,
          createdAt: now,
        },
        $set: {
          tenantId: input.tenantId,
          correlationId: input.correlationId || undefined,
          workflowId: input.workflowId,
          workflowVersionId: input.workflowVersionId,
          status: 'queued',
          attemptCount: 0,
          errorSummary: null,
          startedAt: null,
          finishedAt: null,
          updatedAt: now,
        },
      },
      { upsert: true },
    );

    await queueCollection.updateOne(
      {
        executionId: input.executionId,
        status: { $in: ['queued', 'processing'] },
      },
      {
        $setOnInsert: {
          id: randomUUID(),
          tenantId: input.tenantId,
          executionId: input.executionId,
          createdAt: now,
        },
        $set: {
          availableAt: now,
          status: 'queued',
          retryCount: 0,
          lockedAt: null,
          lockOwner: null,
          updatedAt: now,
        },
      },
      { upsert: true },
    );

    const stepIndexes = input.steps.map((step) => step.stepIndex);
    await stepCollection.deleteMany({
      executionId: input.executionId,
      stepIndex: { $nin: stepIndexes },
    });

    for (const step of input.steps) {
      await stepCollection.updateOne(
        {
          executionId: input.executionId,
          stepIndex: step.stepIndex,
        },
        {
          $setOnInsert: {
            id: randomUUID(),
            createdAt: now,
          },
          $set: {
            stepType: step.type,
            status: 'queued',
            attemptCount: 0,
            requestJson: step,
            responseJson: null,
            errorMessage: null,
            startedAt: null,
            finishedAt: null,
            updatedAt: now,
          },
        },
        { upsert: true },
      );
    }
  }

  async claimAvailableJobs(input: {
    limit: number;
    lockOwner: string;
    lockTimeoutMs: number;
  }): Promise<QueueJob[]> {
    const queueCollection = await getCollection<ExecutionQueueDoc>('ExecutionQueue');
    const now = new Date();
    const staleLockCutoff = new Date(now.getTime() - input.lockTimeoutMs);
    const claimed: QueueJob[] = [];

    for (let i = 0; i < input.limit; i += 1) {
      const doc = await queueCollection.findOneAndUpdate(
        {
          status: 'queued',
          availableAt: { $lte: now },
          $or: [
            { lockedAt: { $exists: false } },
            { lockedAt: null },
            { lockedAt: { $lt: staleLockCutoff } },
          ],
        },
        {
          $set: {
            status: 'processing',
            lockedAt: now,
            lockOwner: input.lockOwner,
          },
        },
        {
          sort: { availableAt: 1 },
          returnDocument: 'after',
        },
      );

      if (!doc) break;

      claimed.push({
        id: this.normalizeId(doc),
        executionId: String(doc.executionId),
        tenantId: String(doc.tenantId),
        status: doc.status,
        retryCount: Number(doc.retryCount || 0),
        availableAt: new Date(doc.availableAt).toISOString(),
      });
    }

    return claimed;
  }

  async getExecutionWithWorkflowDefinition(executionId: string): Promise<ExecutionRecord | null> {
    const executionCollection = await getCollection<ExecutionDoc>('Execution');
    const workflowVersionCollection = await getCollection<WorkflowVersionDoc>('WorkflowVersion');

    const executionDoc = await executionCollection.findOne(this.idFilter<ExecutionDoc>(executionId));
    if (!executionDoc) return null;

    const workflowVersionId = executionDoc.workflowVersionId;
    const workflowVersionDoc = workflowVersionId
      ? await workflowVersionCollection.findOne(this.idFilter<WorkflowVersionDoc>(String(workflowVersionId)))
      : null;

    const definitionJson = (workflowVersionDoc?.definitionJson || {}) as { steps?: unknown };

    return {
      id: this.normalizeId(executionDoc),
      tenantId: String(executionDoc.tenantId),
      correlationId: executionDoc.correlationId,
      status: executionDoc.status,
      attemptCount: Number(executionDoc.attemptCount || 0),
      workflowVersionDefinition: {
        steps: Array.isArray(definitionJson.steps) ? (definitionJson.steps as any) : [],
      },
    };
  }

  async markExecutionRunning(executionId: string): Promise<void> {
    const executionCollection = await getCollection<ExecutionDoc>('Execution');
    const now = new Date();

    await executionCollection.updateOne(this.idFilter<ExecutionDoc>(executionId), [
      {
        $set: {
          status: 'running',
          startedAt: { $ifNull: ['$startedAt', now] },
          updatedAt: now,
        },
      },
    ]);
  }

  async insertStepLog(input: {
    executionId: string;
    stepIndex: number;
    stepType: string;
    status: string;
    attemptCount: number;
    requestJson?: unknown;
    responseJson?: unknown;
    errorMessage?: string;
  }): Promise<void> {
    const stepCollection = await getCollection('ExecutionStep');
    const now = new Date();

    await stepCollection.updateOne(
      {
        executionId: input.executionId,
        stepIndex: input.stepIndex,
      },
      {
        $setOnInsert: {
          id: randomUUID(),
          createdAt: now,
        },
        $set: {
          stepType: input.stepType,
          status: input.status,
          attemptCount: input.attemptCount,
          requestJson: input.requestJson || {},
          responseJson: input.responseJson || {},
          errorMessage: input.errorMessage || null,
          startedAt: input.attemptCount >= 1 ? now : null,
          finishedAt: input.status === 'succeeded' || input.status === 'failed' ? now : null,
          updatedAt: now,
        },
      },
      { upsert: true },
    );
  }

  async finalizeExecution(input: {
    executionId: string;
    queueJobId: string;
    status: 'succeeded' | 'failed';
    errorSummary?: string;
  }): Promise<void> {
    const executionCollection = await getCollection<ExecutionDoc>('Execution');
    const queueCollection = await getCollection<ExecutionQueueDoc>('ExecutionQueue');
    const deadLetterCollection = await getCollection<ExecutionDeadLetterDoc>('ExecutionDeadLetter');
    const now = new Date();

    const queueDoc = await queueCollection.findOne(this.idFilter<ExecutionQueueDoc>(input.queueJobId));
    const executionDoc = await executionCollection.findOne(this.idFilter<ExecutionDoc>(input.executionId));

    await executionCollection.updateOne(this.idFilter<ExecutionDoc>(input.executionId), {
      $set: {
        status: input.status,
        finishedAt: now,
        errorSummary: input.errorSummary || null,
        updatedAt: now,
      },
    });

    await queueCollection.updateOne(this.idFilter<ExecutionQueueDoc>(input.queueJobId), {
      $set: {
        status: input.status === 'succeeded' ? 'completed' : 'failed',
        updatedAt: now,
      },
      $unset: {
        lockedAt: '',
        lockOwner: '',
      },
    });

    if (input.status === 'failed') {
      await deadLetterCollection.updateOne(
        { executionId: input.executionId },
        {
          $setOnInsert: {
            id: randomUUID(),
            executionId: input.executionId,
            firstFailedAt: now,
            replayCount: 0,
            createdAt: now,
          },
          $set: {
            queueJobId: input.queueJobId,
            tenantId: queueDoc?.tenantId,
            correlationId: executionDoc?.correlationId,
            lastErrorSummary: input.errorSummary || undefined,
            status: 'active',
            lastFailedAt: now,
            updatedAt: now,
          },
        },
        { upsert: true },
      );
    }

    if (input.status === 'succeeded') {
      await deadLetterCollection.updateMany(
        {
          executionId: input.executionId,
          status: { $in: ['active', 'replayed'] },
        },
        {
          $set: {
            status: 'resolved',
            updatedAt: now,
          },
        },
      );
    }
  }

  async listExecutionSteps(executionId: string, limit = 100): Promise<ExecutionStepDoc[]> {
    const stepCollection = await getCollection<ExecutionStepDoc>('ExecutionStep');
    const rows = await stepCollection
      .find({ executionId })
      .sort({ stepIndex: 1, updatedAt: -1 })
      .limit(Math.max(1, Math.min(limit, 500)))
      .toArray();

    return rows;
  }

  async listDeadLetters(limit = 100): Promise<DeadLetterRecord[]> {
    const deadLetterCollection = await getCollection<ExecutionDeadLetterDoc>('ExecutionDeadLetter');
    const rows = await deadLetterCollection
      .find({ status: { $in: ['active', 'replayed'] } })
      .sort({ updatedAt: -1 })
      .limit(Math.max(1, Math.min(limit, 500)))
      .toArray();

    return rows.map((row) => this.mapDeadLetter(row));
  }

  async replayDeadLetter(deadLetterId: string): Promise<DeadLetterRecord | null> {
    const deadLetterCollection = await getCollection<ExecutionDeadLetterDoc>('ExecutionDeadLetter');
    const queueCollection = await getCollection<ExecutionQueueDoc>('ExecutionQueue');
    const executionCollection = await getCollection<ExecutionDoc>('Execution');
    const now = new Date();

    const deadLetter = await deadLetterCollection.findOne(this.idFilter<ExecutionDeadLetterDoc>(deadLetterId));
    if (!deadLetter) return null;

    await executionCollection.updateOne(this.idFilter<ExecutionDoc>(deadLetter.executionId), {
      $set: {
        status: 'queued',
        errorSummary: null,
        finishedAt: null,
        updatedAt: now,
      },
    });

    const queueUpdateResult = await queueCollection.updateOne(
      this.idFilter<ExecutionQueueDoc>(deadLetter.queueJobId),
      {
        $set: {
          status: 'queued',
          availableAt: now,
          lockedAt: null,
          lockOwner: null,
          updatedAt: now,
        },
      },
    );

    if (queueUpdateResult.matchedCount === 0) {
      await queueCollection.insertOne({
        id: randomUUID(),
        executionId: deadLetter.executionId,
        tenantId: deadLetter.tenantId || '',
        status: 'queued',
        retryCount: 0,
        availableAt: now,
        lockedAt: null,
        lockOwner: null,
        createdAt: now,
        updatedAt: now,
      });
    }

    const updated = await deadLetterCollection.findOneAndUpdate(
      this.idFilter<ExecutionDeadLetterDoc>(deadLetterId),
      {
        $set: {
          status: 'replayed',
          lastReplayAt: now,
          updatedAt: now,
        },
        $inc: {
          replayCount: 1,
        },
      },
      { returnDocument: 'after' },
    );

    return updated ? this.mapDeadLetter(updated) : null;
  }

  private idFilter<T extends { _id?: ObjectId | string; id?: string }>(id: string): Filter<T> {
    const filters: Filter<T>[] = [{ id } as Filter<T>, { _id: id } as Filter<T>];

    if (ObjectId.isValid(id)) {
      filters.push({ _id: new ObjectId(id) } as Filter<T>);
    }

    return { $or: filters } as Filter<T>;
  }

  private normalizeId(doc: { _id?: ObjectId | string; id?: string }): string {
    if (doc.id) return String(doc.id);
    if (doc._id instanceof ObjectId) return doc._id.toHexString();
    return String(doc._id || '');
  }

  private mapDeadLetter(row: ExecutionDeadLetterDoc): DeadLetterRecord {
    return {
      id: this.normalizeId(row),
      executionId: row.executionId,
      queueJobId: row.queueJobId,
      tenantId: row.tenantId,
      correlationId: row.correlationId,
      lastErrorSummary: row.lastErrorSummary,
      status: row.status,
      firstFailedAt: row.firstFailedAt.toISOString(),
      lastFailedAt: row.lastFailedAt.toISOString(),
      replayCount: Number(row.replayCount || 0),
      lastReplayAt: row.lastReplayAt ? row.lastReplayAt.toISOString() : undefined,
      createdAt: row.createdAt.toISOString(),
      updatedAt: row.updatedAt.toISOString(),
    };
  }
}

export default new ExecutionRepository();
