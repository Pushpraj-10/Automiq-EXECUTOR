import { Request, Response } from 'express';
import config from '../../config/env.js';
import deadLetterService from './deadletter.service.js';

function isAuthorized(req: Request) {
  const secret = String(req.headers['x-executor-secret'] || '');
  return Boolean(secret) && secret === config.executorSharedSecret;
}

export class DeadLetterController {
  listDeadLetters = async (req: Request, res: Response) => {
    if (!isAuthorized(req)) return res.status(401).json({ error: 'Unauthorized' });

    try {
      const limit = req.query.limit ? Number(req.query.limit) : 100;
      const deadLetters = await deadLetterService.listDeadLetters(limit);
      return res.status(200).json({ deadLetters });
    } catch (error: any) {
      return res.status(400).json({ error: error?.message || 'Failed to list dead letters' });
    }
  };

  replayDeadLetter = async (req: Request, res: Response) => {
    if (!isAuthorized(req)) return res.status(401).json({ error: 'Unauthorized' });

    try {
      const deadLetterId = String(req.params.deadLetterId || '');
      const replayed = await deadLetterService.replayDeadLetter(deadLetterId);
      if (!replayed) return res.status(404).json({ error: 'Dead letter not found' });
      return res.status(200).json({ deadLetter: replayed });
    } catch (error: any) {
      return res.status(400).json({ error: error?.message || 'Failed to replay dead letter' });
    }
  };
}

export default new DeadLetterController();
