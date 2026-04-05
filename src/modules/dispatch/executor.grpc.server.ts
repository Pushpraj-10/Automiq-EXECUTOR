import * as grpc from '@grpc/grpc-js';
import fs from 'node:fs';
import config from '../../config/env.js';
import logger from '../../utils/logger.js';
import loadDispatcherProto from './dispatch.grpc.shared.js';
import executionRepository from '../execution/execution.repository.js';
import { parseQueuedAction } from '../../models/action.js';

const proto = loadDispatcherProto();
const executorServiceDefinition = proto.ExecutorService.service;

let grpcServer: grpc.Server | null = null;

type ParsedStep = {
  id: string;
  stepIndex: number;
  type: 'http_request' | 'send_email' | 'webhook_notification' | 'delay';
  name?: string;
  config: Record<string, unknown>;
  onFailure?: {
    strategy?: 'retry' | 'stop' | 'continue';
    maxAttempts?: number;
  };
};

export async function startExecutorGrpcServer() {
  if (grpcServer) return;

  grpcServer = new grpc.Server();

  grpcServer.addService(executorServiceDefinition, {
    EnqueueExecution: async (call: any, callback: any) => {
      try {
        if (!isAuthorized(call.request?.sharedSecret)) {
          return callback({ code: grpc.status.PERMISSION_DENIED, message: 'Unauthorized' });
        }

        const executionId = String(call.request?.executionId || '');
        const correlationId = String(call.request?.correlationId || '');
        const tenantId = String(call.request?.tenantId || '');
        const workflowId = String(call.request?.workflowId || '');
        const workflowVersionId = String(call.request?.workflowVersionId || '');

        if (!executionId || !tenantId || !workflowId || !workflowVersionId) {
          return callback({
            code: grpc.status.INVALID_ARGUMENT,
            message: 'executionId, tenantId, workflowId and workflowVersionId are required',
          });
        }

        const parsedSteps = parseSteps(call.request?.steps || []);

        await executionRepository.enqueueDispatchedExecution({
          executionId,
          tenantId,
          workflowId,
          workflowVersionId,
          correlationId,
          steps: parsedSteps,
        });

        logger.info('Accepted enqueue execution over gRPC', {
          correlationId: correlationId || undefined,
          executionId,
          workflowId,
          stepCount: parsedSteps.length,
        });

        return callback(null, { accepted: true, message: 'Execution queued for batch processing' });
      } catch (error: any) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          message: error?.message || 'Invalid enqueue request',
        });
      }
    },

    GetExecutionSteps: async (call: any, callback: any) => {
      try {
        if (!isAuthorized(call.request?.sharedSecret)) {
          return callback({ code: grpc.status.PERMISSION_DENIED, message: 'Unauthorized' });
        }

        const executionId = String(call.request?.executionId || '');
        const correlationId = String(call.request?.correlationId || '');
        const limit = Number(call.request?.limit || 100);
        if (!executionId) {
          return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'executionId is required' });
        }

        const steps = await executionRepository.listExecutionSteps(executionId, limit);
        logger.info('Fetched execution steps over gRPC', {
          correlationId: correlationId || undefined,
          executionId,
          stepCount: steps.length,
        });
        return callback(null, {
          accepted: true,
          message: 'Execution steps fetched',
          steps: steps.map((step) => ({
            stepIndex: step.stepIndex,
            stepType: step.stepType,
            status: step.status,
            attemptCount: step.attemptCount,
            errorMessage: step.errorMessage || '',
            startedAt: step.startedAt ? new Date(step.startedAt).toISOString() : '',
            finishedAt: step.finishedAt ? new Date(step.finishedAt).toISOString() : '',
            requestJson: JSON.stringify(step.requestJson || {}),
            responseJson: JSON.stringify(step.responseJson || {}),
            updatedAt: step.updatedAt ? new Date(step.updatedAt).toISOString() : '',
          })),
        });
      } catch (error: any) {
        return callback({
          code: grpc.status.INTERNAL,
          message: error?.message || 'Failed to fetch execution steps',
        });
      }
    },
  });

  await new Promise<void>((resolve, reject) => {
    grpcServer!.bindAsync(config.executorGrpcBind, createExecutorServerCredentials(), (error, _port) => {
      if (error) return reject(error);
      resolve();
    });
  });

  grpcServer.start();
  logger.info('Executor gRPC server listening', { bind: config.executorGrpcBind });
}

export async function stopExecutorGrpcServer() {
  if (!grpcServer) return;

  await new Promise<void>((resolve) => {
    grpcServer!.tryShutdown(() => resolve());
  });

  grpcServer = null;
}

function parseSteps(rawSteps: any[]): ParsedStep[] {
  return rawSteps
    .map((step, index) =>
      parseQueuedAction({
        id: step.id,
        stepIndex: Number(step.stepIndex || index + 1),
        type: String(step.type || ''),
        name: step.name ? String(step.name) : undefined,
        config: safeParseJson(step.configJson),
        onFailure: safeParseJson(step.onFailureJson),
        fallbackIndex: index + 1,
      }),
    )
    .sort((a, b) => a.stepIndex - b.stepIndex);
}

function safeParseJson(value: string | undefined) {
  if (!value) return undefined;
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
}

function isAuthorized(sharedSecret: string | undefined) {
  return Boolean(sharedSecret) && sharedSecret === config.executorSharedSecret;
}

function createExecutorServerCredentials() {
  if (!config.executorGrpcTlsEnabled) {
    return grpc.ServerCredentials.createInsecure();
  }

  const serverCert = loadRequiredFile('EXECUTOR_GRPC_SERVER_CERT_PATH', config.executorGrpcServerCertPath);
  const serverKey = loadRequiredFile('EXECUTOR_GRPC_SERVER_KEY_PATH', config.executorGrpcServerKeyPath);
  const rootCert = loadOptionalFile(config.executorGrpcCaCertPath);

  if (config.executorGrpcMtlsEnabled && !rootCert) {
    throw new Error('EXECUTOR_GRPC_CA_CERT_PATH is required when EXECUTOR_GRPC_MTLS_ENABLED=true');
  }

  return grpc.ServerCredentials.createSsl(
    rootCert ?? null,
    [{
      cert_chain: serverCert,
      private_key: serverKey,
    }],
    config.executorGrpcMtlsEnabled,
  );
}

function loadRequiredFile(name: string, filePath: string) {
  const normalized = (filePath || '').trim();
  if (!normalized) {
    throw new Error(`${name} is required when gRPC TLS is enabled`);
  }

  return fs.readFileSync(normalized);
}

function loadOptionalFile(filePath: string | undefined) {
  if (!filePath) return undefined;
  const normalized = filePath.trim();
  if (!normalized) return undefined;
  return fs.readFileSync(normalized);
}

export default {
  startExecutorGrpcServer,
  stopExecutorGrpcServer,
};
