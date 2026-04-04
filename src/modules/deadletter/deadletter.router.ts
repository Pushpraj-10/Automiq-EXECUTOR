import { Router } from 'express';
import deadLetterController from './deadletter.controller.js';

const deadLetterRouter = Router();

deadLetterRouter.get('/', deadLetterController.listDeadLetters);
deadLetterRouter.post('/:deadLetterId/replay', deadLetterController.replayDeadLetter);

export default deadLetterRouter;
