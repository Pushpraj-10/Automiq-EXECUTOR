import * as grpc from '@grpc/grpc-js';
import fs from 'node:fs';
import config from '../../config/env.js';
import logger from '../../utils/logger.js';
import loadDispatcherProto from './dispatch.grpc.shared.js';

type DispatchStatus = 'queued' | 'running' | 'succeeded' | 'failed';
type DispatchStepStatus = 'queued' | 'running' | 'succeeded' | 'failed';

const proto = loadDispatcherProto();
const DispatcherService = proto.DispatcherService;
const dispatcherClient = new DispatcherService(
  config.backendGrpcAddress,
  createBackendClientCredentials(),
) as any;

const grpcCircuitState = {
  consecutiveFailures: 0,
  openUntilEpochMs: 0,
};

export async function reportExecutionStatus(input: {
  correlationId?: string;
  executionId: string;
  status: DispatchStatus;
  errorSummary?: string;
}) {
  await callWithRetry('UpdateExecutionStatus', () => {
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
  await callWithRetry('UpdateExecutionStepStatus', () => {
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
  });
}

function createBackendClientCredentials() {
  if (!config.backendGrpcTlsEnabled) {
    return grpc.credentials.createInsecure();
  }

  const caCert = loadOptionalFile(config.backendGrpcCaCertPath);
  const clientCert = loadOptionalFile(config.backendGrpcClientCertPath);
  const clientKey = loadOptionalFile(config.backendGrpcClientKeyPath);

  const hasMtlsPair = Boolean(clientCert && clientKey);
  if ((clientCert && !clientKey) || (!clientCert && clientKey)) {
    throw new Error('BACKEND_GRPC_CLIENT_CERT_PATH and BACKEND_GRPC_CLIENT_KEY_PATH must both be set for mTLS');
  }

  return grpc.credentials.createSsl(caCert, hasMtlsPair ? clientKey : undefined, hasMtlsPair ? clientCert : undefined);
}

async function callWithRetry<T>(operationName: string, operation: () => Promise<T>): Promise<T> {
  const now = Date.now();
  if (grpcCircuitState.openUntilEpochMs > now) {
    throw new Error(`Backend gRPC circuit breaker open for ${operationName}`);
  }

  let lastError: unknown;
  for (let attempt = 1; attempt <= config.backendGrpcRetryAttempts; attempt += 1) {
    try {
      const result = await operation();
      grpcCircuitState.consecutiveFailures = 0;
      grpcCircuitState.openUntilEpochMs = 0;
      return result;
    } catch (error) {
      lastError = error;
      grpcCircuitState.consecutiveFailures += 1;

      if (grpcCircuitState.consecutiveFailures >= config.backendGrpcCircuitBreakerFailureThreshold) {
        grpcCircuitState.openUntilEpochMs = Date.now() + config.backendGrpcCircuitBreakerResetMs;
      }

      const shouldRetry = attempt < config.backendGrpcRetryAttempts && isRetryableGrpcError(error);
      if (!shouldRetry) {
        break;
      }

      const backoffMs = config.backendGrpcRetryBaseMs * Math.pow(2, attempt - 1);
      const jitterMs = Math.floor(Math.random() * Math.max(25, config.backendGrpcRetryBaseMs));
      await delay(backoffMs + jitterMs);
    }
  }

  throw lastError instanceof Error ? lastError : new Error(`Backend gRPC ${operationName} failed`);
}

function isRetryableGrpcError(error: unknown) {
  const code = Number((error as grpc.ServiceError | undefined)?.code);
  return (
    code === grpc.status.UNAVAILABLE ||
    code === grpc.status.DEADLINE_EXCEEDED ||
    code === grpc.status.RESOURCE_EXHAUSTED ||
    code === grpc.status.INTERNAL ||
    code === grpc.status.UNKNOWN
  );
}

function loadOptionalFile(filePath: string | undefined) {
  if (!filePath) return undefined;
  const normalized = filePath.trim();
  if (!normalized) return undefined;
  return fs.readFileSync(normalized);
}

async function delay(ms: number) {
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

export default {
  reportExecutionStatus,
  reportExecutionStepStatus,
};
