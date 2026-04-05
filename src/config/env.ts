import 'dotenv/config';

function required(name: string, value?: string) {
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

function asNumber(name: string, value: string | undefined, fallback: number) {
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`Invalid number for ${name}`);
  return parsed;
}

function asBoolean(value: string | undefined, fallback = false) {
  if (value === undefined) return fallback;
  const normalized = value.trim().toLowerCase();
  if (normalized === 'true') return true;
  if (normalized === 'false') return false;
  return fallback;
}

export const config = {
  nodeEnv: (process.env.NODE_ENV || 'development') as 'development' | 'production' | 'test',
  port: asNumber('EXECUTOR_PORT', process.env.EXECUTOR_PORT, 4001),
  mongodbUri: required('MONGODB_URI', process.env.MONGODB_URI),
  mongodbDbName: process.env.MONGODB_DB_NAME || 'automiq',
  executorGrpcBind: process.env.EXECUTOR_GRPC_BIND || '0.0.0.0:50051',
  executorGrpcTlsEnabled: asBoolean(process.env.EXECUTOR_GRPC_TLS_ENABLED, false),
  executorGrpcMtlsEnabled: asBoolean(process.env.EXECUTOR_GRPC_MTLS_ENABLED, false),
  executorGrpcServerCertPath: process.env.EXECUTOR_GRPC_SERVER_CERT_PATH || '',
  executorGrpcServerKeyPath: process.env.EXECUTOR_GRPC_SERVER_KEY_PATH || '',
  executorGrpcCaCertPath: process.env.EXECUTOR_GRPC_CA_CERT_PATH || '',
  backendGrpcAddress: process.env.BACKEND_GRPC_ADDRESS || '127.0.0.1:50052',
  backendGrpcTlsEnabled: asBoolean(process.env.BACKEND_GRPC_TLS_ENABLED, false),
  backendGrpcCaCertPath: process.env.BACKEND_GRPC_CA_CERT_PATH || '',
  backendGrpcClientCertPath: process.env.BACKEND_GRPC_CLIENT_CERT_PATH || '',
  backendGrpcClientKeyPath: process.env.BACKEND_GRPC_CLIENT_KEY_PATH || '',
  backendGrpcRetryAttempts: asNumber('BACKEND_GRPC_RETRY_ATTEMPTS', process.env.BACKEND_GRPC_RETRY_ATTEMPTS, 3),
  backendGrpcRetryBaseMs: asNumber('BACKEND_GRPC_RETRY_BASE_MS', process.env.BACKEND_GRPC_RETRY_BASE_MS, 200),
  backendGrpcCircuitBreakerFailureThreshold: asNumber(
    'BACKEND_GRPC_CIRCUIT_BREAKER_FAILURE_THRESHOLD',
    process.env.BACKEND_GRPC_CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    5,
  ),
  backendGrpcCircuitBreakerResetMs: asNumber(
    'BACKEND_GRPC_CIRCUIT_BREAKER_RESET_MS',
    process.env.BACKEND_GRPC_CIRCUIT_BREAKER_RESET_MS,
    10_000,
  ),
  executorSharedSecret: process.env.EXECUTOR_SHARED_SECRET || 'executor-dev-secret',
  sendgridApiKey: process.env.SENDGRID_API_KEY || '',
  smtpHost: process.env.SMTP_HOST || '',
  smtpPort: asNumber('SMTP_PORT', process.env.SMTP_PORT, 587),
  smtpUser: process.env.SMTP_USER || '',
  smtpPass: process.env.SMTP_PASS || '',
  smtpSecure: String(process.env.SMTP_SECURE || 'false').toLowerCase() === 'true',
  smtpDefaultFrom: process.env.SMTP_DEFAULT_FROM || '',
  sendgridConfigured: Boolean((process.env.SENDGRID_API_KEY || '').trim()),
  smtpConfigured: Boolean((process.env.SMTP_HOST || '').trim() && (process.env.SMTP_USER || '').trim() && (process.env.SMTP_PASS || '').trim()),
  emailDefaultFromConfigured: Boolean((process.env.SMTP_DEFAULT_FROM || '').trim()),
  workerPollIntervalMs: asNumber('WORKER_POLL_INTERVAL_MS', process.env.WORKER_POLL_INTERVAL_MS, 3000),
  workerBatchSize: asNumber('WORKER_BATCH_SIZE', process.env.WORKER_BATCH_SIZE, 5),
  queueLockTimeoutMs: asNumber('QUEUE_LOCK_TIMEOUT_MS', process.env.QUEUE_LOCK_TIMEOUT_MS, 60000),
};

export default config;
