import { Collection, Db, Document, MongoClient } from 'mongodb';
import config from './env.js';

const client = new MongoClient(config.mongodbUri, {
  maxPoolSize: 20,
});

let db: Db | null = null;
let connectPromise: Promise<Db> | null = null;

async function ensureIndexes(database: Db) {
  await database.collection('Execution').createIndex({ id: 1 }, { unique: true, sparse: true });
  await database.collection('WorkflowVersion').createIndex({ id: 1 }, { unique: true, sparse: true });
  await database.collection('ExecutionQueue').createIndex({ id: 1 }, { unique: true, sparse: true });
  await database.collection('ExecutionQueue').createIndex({ status: 1, availableAt: 1, lockedAt: 1 });
  await database.collection('ExecutionQueue').createIndex({ executionId: 1, status: 1 });
  await database.collection('ExecutionStep').createIndex({ executionId: 1, stepIndex: 1, createdAt: 1 });
  await database.collection('ExecutionStep').createIndex({ executionId: 1, stepIndex: 1 }, { unique: true });
  await database.collection('Execution').createIndex({ status: 1, updatedAt: -1 });
  await database.collection('ExecutionDeadLetter').createIndex({ id: 1 }, { unique: true, sparse: true });
  await database.collection('ExecutionDeadLetter').createIndex({ executionId: 1 }, { unique: true });
  await database.collection('ExecutionDeadLetter').createIndex({ status: 1, updatedAt: -1 });
}

export async function connectDb(): Promise<Db> {
  if (db) return db;
  if (connectPromise) return connectPromise;

  connectPromise = (async () => {
    await client.connect();
    const database = client.db(config.mongodbDbName);
    await ensureIndexes(database);
    db = database;
    return database;
  })();

  try {
    return await connectPromise;
  } finally {
    connectPromise = null;
  }
}

export async function getDb(): Promise<Db> {
  return connectDb();
}

export async function getCollection<T extends Document = Document>(name: string): Promise<Collection<T>> {
  const database = await connectDb();
  return database.collection<T>(name);
}

export async function closeDb() {
  db = null;
  await client.close();
}

export default {
  connectDb,
  getDb,
  getCollection,
  closeDb,
};
