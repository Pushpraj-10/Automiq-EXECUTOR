import http from 'node:http';
import createApp from './app.js';
import config from './config/env.js';
import logger from './utils/logger.js';
import executionWorker from './modules/execution/execution.worker.js';
import { closeDb } from './config/db.js';
import { startExecutorGrpcServer, stopExecutorGrpcServer } from './modules/dispatch/executor.grpc.server.js';

let server: http.Server | null = null;

export async function startServer() {
  if (server) return server;

  const app = createApp();
  server = http.createServer(app);

  await new Promise<void>((resolve, reject) => {
    server!.listen(config.port, () => {
      logger.info('Executor server listening', { port: config.port });
      resolve();
    });
    server!.on('error', reject);
  });

  executionWorker.start();
  await startExecutorGrpcServer();

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  return server;
}

export async function stopServer() {
  await executionWorker.stop();
  await stopExecutorGrpcServer();
  await closeDb();

  if (!server) return;

  await new Promise<void>((resolve, reject) => {
    server!.close((err) => {
      if (err) return reject(err);
      resolve();
    });
  });

  server = null;
}

async function shutdown(signal: string) {
  logger.info('Executor shutdown initiated', { signal });
  try {
    await stopServer();
    logger.info('Executor shutdown complete');
    process.exit(0);
  } catch (error: any) {
    logger.error('Executor shutdown failed', { error: error?.message || String(error) });
    process.exit(1);
  }
}

startServer().catch((error: any) => {
  logger.error('Executor failed to start', { error: error?.message || String(error) });
  process.exit(1);
});
