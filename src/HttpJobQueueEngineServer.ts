import type { IncomingMessage, ServerResponse } from 'http';
import type { JobQueueEngine } from './JobQueueEngine';
import { pack, unpack } from './packer';

function readPacked(req: IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
    req.on('end', () => {
      try {
        if (chunks.length === 0) return resolve({});
        const buf = Buffer.concat(chunks);
        if (buf.length === 0) return resolve({});
        resolve(unpack(buf));
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

function sendPacked(res: ServerResponse, status: number, obj: unknown) {
  const payload = pack(obj);
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Length', payload.length);
  res.end(payload);
}

function sendNoContent(res: ServerResponse) {
  res.statusCode = 204;
  res.end();
}

function errorResponse(res: ServerResponse, err: unknown) {
  const msg = String(err instanceof Error ? err.message : err);
  const status = msg === 'NotImplemented' ? 501 : 500;
  sendPacked(res, status, { error: msg });
}

export class HttpJobQueueEngineServer {
  constructor(private readonly engine: JobQueueEngine, private readonly basePath = '/engine') {}

  async submitJobHandler(req: IncomingMessage, res: ServerResponse) {
    try {
      const body = await readPacked(req);
      if (body?.statusHandler != null) {
        throw Error('NotImplemented');
      }
      await this.engine.submitJob({
        data: body.data,
        queue: body.queue,
        parentId: body.parentId,
        parentQueue: body.parentQueue,
        dataKey: body.dataKey,
      });
      sendNoContent(res);
    } catch (err) {
      errorResponse(res, err);
    }
  }

  async subscribeToJobStatusHandler(_req: IncomingMessage, res: ServerResponse) {
    try {
      throw Error('NotImplemented');
    } catch (err) {
      errorResponse(res, err);
    }
  }

  async acquireJobHandler(req: IncomingMessage, res: ServerResponse) {
    try {
      const body = await readPacked(req);
      const result = await this.engine.acquireJob({
        queue: body.queue,
        token: body.token,
        block: body.block,
      } as any);
      sendPacked(res, 200, result);
    } catch (err) {
      errorResponse(res, err);
    }
  }

  async completeJobHandler(req: IncomingMessage, res: ServerResponse) {
    try {
      const body = await readPacked(req);
      await this.engine.completeJob({
        queue: body.queue,
        token: body.token,
        jobId: body.jobId,
        result: body.result,
      });
      sendNoContent(res);
    } catch (err) {
      errorResponse(res, err);
    }
  }

  async updateJobHandler(req: IncomingMessage, res: ServerResponse) {
    try {
      const body = await readPacked(req);
      const result = await this.engine.updateJob({
        queue: body.queue,
        token: body.token,
        jobId: body.jobId,
        lockTimeoutMs: body.lockTimeoutMs,
        progressInfo: body.progressInfo,
        input: body.input,
      });
      sendPacked(res, 200, result);
    } catch (err) {
      errorResponse(res, err);
    }
  }

  async getJobResultHandler(req: IncomingMessage, res: ServerResponse) {
    try {
      const body = await readPacked(req);
      const result = await this.engine.getJobResult({
        resultKey: body.resultKey,
        delete: body.delete,
      } as any);
      sendPacked(res, 200, result);
    } catch (err) {
      errorResponse(res, err);
    }
  }

  async submitChildrenSuspendParentHandler(req: IncomingMessage, res: ServerResponse) {
    try {
      const body = await readPacked(req);
      const result = await this.engine.submitChildrenSuspendParent({
        children: body.children,
        token: body.token,
        parentId: body.parentId,
        parentQueue: body.parentQueue,
      } as any);
      sendPacked(res, 200, result === undefined ? null : result);
    } catch (err) {
      errorResponse(res, err);
    }
  }

  handler = async (req: IncomingMessage, res: ServerResponse) => {
    const url = req.url || '';
    if (req.method !== 'POST') {
      sendPacked(res, 405, { error: 'method not allowed' });
      return;
    }
    const path = url.split('?')[0] || '';
    const base = this.basePath.endsWith('/') ? this.basePath.slice(0, -1) : this.basePath;
    switch (path) {
      case `${base}/submitJob`: return this.submitJobHandler(req, res);
      case `${base}/subscribeToJobStatus`: return this.subscribeToJobStatusHandler(req, res);
      case `${base}/acquireJob`: return this.acquireJobHandler(req, res);
      case `${base}/completeJob`: return this.completeJobHandler(req, res);
      case `${base}/updateJob`: return this.updateJobHandler(req, res);
      case `${base}/getJobResult`: return this.getJobResultHandler(req, res);
      case `${base}/submitChildrenSuspendParent`: return this.submitChildrenSuspendParentHandler(req, res);
      default:
        sendPacked(res, 404, { error: 'not found' });
        return;
    }
  };
}
