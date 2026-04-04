import express from 'express';
import deadLetterRouter from './modules/deadletter/deadletter.router.js';

export function createApp() {
  const app = express();

  app.disable('x-powered-by');
  app.use(express.json({ limit: '1mb' }));

  app.get('/health', (_req, res) => {
    res.status(200).json({ ok: true, service: 'executor' });
  });

  app.use('/ops/dead-letters', deadLetterRouter);

  return app;
}

export default createApp;
