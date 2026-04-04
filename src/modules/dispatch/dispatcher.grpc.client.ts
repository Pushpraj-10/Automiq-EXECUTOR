import * as grpc from '@grpc/grpc-js';
import config from '../../config/env.js';
import logger from '../../utils/logger.js';
import loadDispatcherProto from './dispatch.grpc.shared.js';

type DispatchStatus = 'queued' | 'running' | 'succeeded' | 'failed';
type DispatchStepStatus = 'queued' | 'running' | 'succeeded' | 'failed';

const proto = loadDispatcherProto();
const DispatcherService = proto.DispatcherService;
const dispatcherClient = new DispatcherService(config.backendGrpcAddress, grpc.credentials.createInsecure()) as any;

export async function reportExecutionStatus(input: {
  correlationId?: string;
  executionId: string;
  status: DispatchStatus;
  errorSummary?: string;
}) {
  return new Promise<void>((resolve, reject) => {
    dispatcherClient.UpdateExecutionStatus(
      {
        sharedSecret: config.executorSharedSecret,
        correlationId: input.correlationId || '',
        executionId: input.executionId,
        status: input.status,
        errorSummary: input.errorSummary || '',
      },
      (error: grpc.ServiceError | null, response: any) => {
        if (error) {
          logger.error('Failed to report execution status via gRPC', {
            executionId: input.executionId,
            status: input.status,
            error: error.message,
          });
          return reject(error);
        }

        if (!response?.accepted) {
          return reject(new Error(response?.message || 'Dispatcher rejected execution status update'));
        }

        return resolve();
      },
    );
  });
}

export async function reportExecutionStepStatus(input: {
  correlationId?: string;
  executionId: string;
  stepIndex: number;
  status: DispatchStepStatus;
  stepType: string;
  attemptCount: number;
  requestJson?: unknown;
  responseJson?: unknown;
  errorMessage?: string;
}) {
  return new Promise<void>((resolve, reject) => {
    dispatcherClient.UpdateExecutionStepStatus(
      {
        sharedSecret: config.executorSharedSecret,
        correlationId: input.correlationId || '',
        executionId: input.executionId,
        stepIndex: input.stepIndex,
        status: input.status,
        stepType: input.stepType,
        attemptCount: input.attemptCount,
        requestJson: JSON.stringify(input.requestJson || {}),
        responseJson: JSON.stringify(input.responseJson || {}),
        errorMessage: input.errorMessage || '',
      },
      (error: grpc.ServiceError | null, response: any) => {
        if (error) {
          logger.error('Failed to report execution step status via gRPC', {
            executionId: input.executionId,
            stepIndex: input.stepIndex,
            status: input.status,
            error: error.message,
          });
          return reject(error);
        }

        if (!response?.accepted) {
          return reject(new Error(response?.message || 'Dispatcher rejected execution step status update'));
        }

        return resolve();
      },
    );
  });
}

export default {
  reportExecutionStatus,
  reportExecutionStepStatus,
};
