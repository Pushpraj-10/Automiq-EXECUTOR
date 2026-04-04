import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

function resolveProtoPath() {
  const currentDir = path.dirname(fileURLToPath(import.meta.url));
  const candidates = [
    process.env.DISPATCHER_PROTO_PATH,
    path.resolve(process.cwd(), '../proto/dispatcher_executor.proto'),
    path.resolve(process.cwd(), 'proto/dispatcher_executor.proto'),
    path.resolve(currentDir, '../../../../proto/dispatcher_executor.proto'),
  ].filter((value): value is string => Boolean(value));

  const found = candidates.find((candidate) => fs.existsSync(candidate));
  if (!found) {
    throw new Error(`Unable to resolve dispatcher proto. Checked: ${candidates.join(', ')}`);
  }

  return found;
}

export function loadDispatcherProto() {
  const packageDefinition = protoLoader.loadSync(resolveProtoPath(), {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  const loaded = grpc.loadPackageDefinition(packageDefinition) as any;
  return loaded.automiq.dispatcher;
}

export default loadDispatcherProto;
