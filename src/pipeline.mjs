// 采集管线:把多个慢速分块的 gzip NDJSON 来源采到按批确认的下游。
//
// 背压贯通路径:
//   socket(原始字节预算)→ gunzip(解压字节预算)→ 解析(单条上限与独立策略)
//   → 批次(待提交记录/字节预算)→ 发件箱(持久化)→ 提交器(查证 + 重试)。
// 每一跳都是拉取式:预算不批就不拉下一块,网络侧由 TCP 背压自然限速,
// 来源之间互不占线——一条慢流只占用自己的连接与已领预算。
//
// 取消语义:
//   close() 优雅关闭 —— 停止继续领取,部分批次入库,收拢仍可核对的提交;
//   abort() 硬中止   —— 模拟进程被杀,状态以发件箱为准,留给下次恢复。

import http from 'node:http';
import {createGunzip} from 'node:zlib';
import {once} from 'node:events';
import {setTimeout as delay} from 'node:timers/promises';
import {ResourceBudget} from './budget.mjs';
import {parseNdjson} from './ndjson.mjs';
import {Outbox, fingerprintOf, batchIdOf} from './outbox.mjs';

export class PipelineClosed extends Error {
  constructor(message = '采集链路已关闭') {
    super(message);
    this.name = 'PipelineClosed';
  }
}

export class SourceIdleTimeout extends Error {
  constructor(source, ms) {
    super(`来源 ${source} 超过 ${ms}ms 没有产出字节`);
    this.name = 'SourceIdleTimeout';
  }
}

class BatchRejected extends Error {}
class BatchConflict extends Error {}

export function createPipeline(options) {
  const {
    sources,
    sinkUrl,
    outboxPath,
    budget: budgetOption,
    batchRecords = 100,
    batchBytes = 256 * 1024,
    maxRecordBytes = 1024 * 1024,
    oversized = 'skip',
    sourceIdleTimeoutMs = 30_000,
    commitTimeoutMs = 5_000,
    commitRetries = 3,
    commitBackoffMs = 200,
    fetchImpl = fetch,
    onEvent = () => {},
  } = options;

  if (!Array.isArray(sources) || sources.length === 0) throw new Error('至少需要一个来源');
  if (!sinkUrl) throw new Error('缺少 sinkUrl');
  if (!outboxPath) throw new Error('缺少 outboxPath');
  if (!['skip', 'fail'].includes(oversized)) throw new Error(`未知的超大记录策略: ${oversized}`);

  const budget = budgetOption instanceof ResourceBudget ? budgetOption : new ResourceBudget(budgetOption);
  const limits = budget.limits();
  for (const dim of ['rawBytes', 'inflatedBytes', 'pendingRecords', 'pendingBytes', 'connections']) {
    if (!(dim in limits)) throw new Error(`预算缺少维度: ${dim}`);
  }
  // 配置一致性:单批与单条必须能放进待提交预算,否则正常路径必然卡死。
  if (batchRecords > limits.pendingRecords) {
    throw new Error(`batchRecords(${batchRecords}) 不能超过 pendingRecords 预算(${limits.pendingRecords})`);
  }
  if (batchBytes > limits.pendingBytes) {
    throw new Error(`batchBytes(${batchBytes}) 不能超过 pendingBytes 预算(${limits.pendingBytes})`);
  }
  if (maxRecordBytes > limits.pendingBytes) {
    throw new Error(`maxRecordBytes(${maxRecordBytes}) 不能超过 pendingBytes 预算(${limits.pendingBytes})`);
  }

  const emit = event => {
    try {
      onEvent({time: new Date().toISOString(), ...event});
    } catch {}
  };

  const state = {
    started: false,
    startedAt: 0,
    closing: false,
    aborted: false,
    sinkDown: false,
    sourcesSettled: false,
    finished: false,
    commitQueue: [],
    inflight: 0,
    finishResolve: null,
    workersSettled: null,
    committerDone: null,
  };
  state.finishedPromise = new Promise(resolve => {
    state.finishResolve = resolve;
  });

  const infos = new Map(
    sources.map(source => [
      source.name,
      {
        status: 'pending',
        parsed: 0,
        skipped: 0,
        invalid: 0,
        oversized: 0,
        batches: 0,
        committedRecords: 0,
        bytesRaw: 0,
        bytesInflated: 0,
        waitingOn: null,
        error: null,
      },
    ]),
  );
  const counters = {committed: 0, verified: 0, rejected: 0, conflicts: 0, retries: 0};

  let outbox = null;
  const workerControllers = new Map(); // name -> {controller, idleTimer}
  const pipelineController = new AbortController(); // 硬中止时一并终止下游请求

  // 提交器的门闩:有批次入队或状态变化时唤醒。
  let poke = null;
  let poked = false;
  const wakeCommitter = () => {
    if (poke) {
      poke();
      poke = null;
    } else {
      poked = true;
    }
  };
  const waitCommitter = async () => {
    if (poked) {
      poked = false;
      return;
    }
    await new Promise(resolve => {
      poke = resolve;
    });
  };

  async function acquireDim(dim, amount, {owner, signal, force} = {}) {
    const info = infos.get(owner);
    if (info) info.waitingOn = {dim, amount, since: Date.now()};
    try {
      await budget.acquire(dim, amount, {owner, signal, force});
    } finally {
      if (info) info.waitingOn = null;
    }
  }

  function releaseDim(dim, amount, owner) {
    budget.release(dim, amount, owner);
  }

  function httpGet(url, signal) {
    return new Promise((resolve, reject) => {
      const request = http.request(url, {headers: {'accept-encoding': 'identity'}, signal}, resolve);
      request.on('error', reject);
      request.end();
    });
  }

  // 原始字节预算:按预算上限把网络块切片领取,预算不批就不继续拉 socket。
  async function* rawChunks(response, sourceName, signal, onChunk) {
    const limit = limits.rawBytes;
    response.on('error', () => {}); // 迭代器自身会拿到错误,这里兜底避免游离的 error 事件
    try {
      for await (const chunk of response) {
        onChunk();
        infos.get(sourceName).bytesRaw += chunk.length;
        let offset = 0;
        while (offset < chunk.length) {
          const piece = Math.min(chunk.length - offset, limit);
          await acquireDim('rawBytes', piece, {owner: sourceName, signal});
          try {
            yield chunk.subarray(offset, offset + piece);
          } finally {
            releaseDim('rawBytes', piece, sourceName);
          }
          offset += piece;
        }
      }
    } finally {
      response.destroy();
    }
  }

  // 手动把原始块喂给 gunzip:每片写入并等 drain,rawBytes 预算因此持有到
  // gunzip 真正消费为止;不经过 Readable.from/pipe 的预读缓冲,预算没有暗渠。
  async function* gunzipOutputs(raw, signal) {
    const gunzip = createGunzip();
    const feed = (async () => {
      try {
        for await (const piece of raw) {
          if (gunzip.destroyed) return;
          if (!gunzip.write(piece)) {
            await Promise.race([once(gunzip, 'drain'), once(gunzip, 'close')]).catch(() => {});
          }
        }
        gunzip.end();
      } catch {
        gunzip.destroy();
      }
    })();
    feed.catch(() => {}); // 错误在读取侧体现,这里避免未处理拒绝
    try {
      for await (const chunk of gunzip) {
        yield chunk;
      }
    } finally {
      gunzip.destroy();
      await raw.return?.();
      await feed.catch(() => {});
    }
  }

  // 解压字节预算:每个输出块先领预算再交给解析器。
  async function* inflatedChunks(raw, contentEncoding, sourceName, signal) {
    const limit = limits.inflatedBytes;
    const gzip = String(contentEncoding ?? '').toLowerCase().includes('gzip');
    const outputs = gzip ? gunzipOutputs(raw, signal) : raw;
    try {
      for await (const chunk of outputs) {
        infos.get(sourceName).bytesInflated += chunk.length;
        let offset = 0;
        while (offset < chunk.length) {
          const piece = Math.min(chunk.length - offset, limit);
          await acquireDim('inflatedBytes', piece, {owner: sourceName, signal});
          try {
            yield chunk.subarray(offset, offset + piece);
          } finally {
            releaseDim('inflatedBytes', piece, sourceName);
          }
          offset += piece;
        }
      }
    } finally {
      await outputs.return?.();
    }
  }

  function newBatch(sourceName) {
    return {source: sourceName, firstOffset: null, lastOffset: null, records: [], bytes: 0};
  }

  // 暂存批次:先写发件箱(落盘),再交给提交器;提交器已停时只入库并释放预算。
  async function stageBatch(sourceName, batch) {
    const fingerprint = fingerprintOf(batch.records);
    const record = {
      type: 'batch',
      batchId: batchIdOf(sourceName, batch.firstOffset, batch.lastOffset, fingerprint),
      source: sourceName,
      firstOffset: batch.firstOffset,
      lastOffset: batch.lastOffset,
      recordCount: batch.records.length,
      bytes: batch.bytes,
      fingerprint,
      records: batch.records,
      stagedAt: new Date().toISOString(),
    };
    await outbox.stage(record);
    infos.get(sourceName).batches += 1;
    if (state.abortCommitter || state.sinkDown) {
      releaseDim('pendingRecords', record.recordCount, sourceName);
      releaseDim('pendingBytes', record.bytes, sourceName);
      return;
    }
    state.commitQueue.push({batch: record, held: true, recovered: false});
    wakeCommitter();
  }

  async function runSource(source) {
    const info = infos.get(source.name);
    const controller = new AbortController();
    const worker = {controller, idleTimer: null};
    workerControllers.set(source.name, worker);
    const refreshIdle = () => {
      if (sourceIdleTimeoutMs > 0) {
        clearTimeout(worker.idleTimer);
        worker.idleTimer = setTimeout(
          () => controller.abort(new SourceIdleTimeout(source.name, sourceIdleTimeoutMs)),
          sourceIdleTimeoutMs,
        );
      }
    };
    info.status = 'connecting';
    let connected = false;
    let pendingBatch = newBatch(source.name);
    try {
      await acquireDim('connections', 1, {owner: source.name, signal: controller.signal});
      connected = true;
      refreshIdle();
      const response = await httpGet(new URL(source.path ?? '/stream', source.url), controller.signal);
      if (response.statusCode !== 200) {
        response.resume();
        throw new Error(`来源响应状态 ${response.statusCode}`);
      }
      info.status = 'streaming';
      emit({type: 'sourceStart', source: source.name});
      const skipBelow = outbox.checkpointFor(source.name);
      const raw = rawChunks(response, source.name, controller.signal, refreshIdle);
      const inflated = inflatedChunks(raw, response.headers['content-encoding'], source.name, controller.signal);
      const records = parseNdjson(inflated, {
        maxRecordBytes,
        oversized,
        onOversized: async report => {
          info.oversized += 1;
          emit({type: 'recordOversized', source: source.name, ...report});
        },
      });
      for await (const record of records) {
        if (record.end <= skipBelow) {
          info.skipped += 1; // 已确认或已入库的区间,恢复时不重做
          continue;
        }
        info.parsed += 1;
        await acquireDim('pendingRecords', 1, {owner: source.name, signal: controller.signal});
        await acquireDim('pendingBytes', record.body.length, {owner: source.name, signal: controller.signal});
        let parsed;
        try {
          parsed = JSON.parse(record.body.toString('utf8'));
        } catch {
          info.invalid += 1;
          emit({type: 'recordInvalid', source: source.name, offset: record.start, bytes: record.body.length});
          releaseDim('pendingRecords', 1, source.name);
          releaseDim('pendingBytes', record.body.length, source.name);
          continue;
        }
        if (
          pendingBatch.records.length > 0 &&
          (pendingBatch.records.length >= batchRecords || pendingBatch.bytes + record.body.length > batchBytes)
        ) {
          await stageBatch(source.name, pendingBatch);
          pendingBatch = newBatch(source.name);
        }
        if (pendingBatch.firstOffset === null) pendingBatch.firstOffset = record.start;
        pendingBatch.records.push(parsed);
        pendingBatch.bytes += record.body.length;
        pendingBatch.lastOffset = record.end;
      }
      if (pendingBatch.records.length > 0) {
        await stageBatch(source.name, pendingBatch);
        pendingBatch = newBatch(source.name);
      }
      info.status = 'done';
      emit({type: 'sourceDone', source: source.name, records: info.parsed, skipped: info.skipped});
    } catch (error) {
      const reason = controller.signal.reason;
      if (controller.signal.aborted && reason instanceof SourceIdleTimeout) {
        info.status = 'timeout';
        info.error = reason.message;
        emit({type: 'sourceTimeout', source: source.name, message: reason.message});
      } else if (controller.signal.aborted) {
        info.status = 'stopped';
      } else {
        info.status = 'error';
        info.error = String(error?.message ?? error);
        emit({type: 'sourceError', source: source.name, error: info.error});
      }
    } finally {
      clearTimeout(worker.idleTimer);
      // 收拢:未成行的部分批次照样入库,等待提交或下次恢复,不丢已领取的工作。
      if (pendingBatch.records.length > 0) {
        try {
          await stageBatch(source.name, pendingBatch);
        } catch (error) {
          emit({type: 'outboxError', source: source.name, error: String(error?.message ?? error)});
        }
      }
      if (connected) releaseDim('connections', 1, source.name);
      workerControllers.delete(source.name);
    }
  }

  function releaseJobBudget(job) {
    if (!job.held) return;
    releaseDim('pendingRecords', job.batch.recordCount, job.batch.source);
    releaseDim('pendingBytes', job.batch.bytes, job.batch.source);
    job.held = false;
  }

  async function confirmBatch(job, how, receipt = null) {
    const {batch} = job;
    await outbox.markDone(batch.batchId, receipt ?? batch.receipt ?? null);
    releaseJobBudget(job);
    infos.get(batch.source).committedRecords += batch.recordCount;
    if (how === 'verified') {
      counters.verified += 1;
      emit({type: 'batchVerified', batchId: batch.batchId, source: batch.source, records: batch.recordCount});
    } else {
      counters.committed += 1;
      emit({type: 'batchCommitted', batchId: batch.batchId, source: batch.source, records: batch.recordCount});
    }
  }

  async function failBatch(job, reason) {
    const {batch} = job;
    await outbox.markFailed(batch.batchId, reason);
    releaseJobBudget(job);
  }

  // 查证未知提交:下游可能有(确认丢失)也可能没有(请求未达)。
  async function verifyBatch(batch) {
    try {
      const response = await fetchImpl(`${sinkUrl}/sink/${batch.batchId}`, {
        signal: AbortSignal.any([AbortSignal.timeout(commitTimeoutMs), pipelineController.signal]),
      });
      if (response.status === 404) return 'absent';
      if (!response.ok) return 'unknown';
      const receipt = await response.json();
      if (receipt.fingerprint === batch.fingerprint) {
        batch.receipt = receipt;
        return 'done';
      }
      return 'conflict';
    } catch {
      return 'unknown';
    }
  }

  async function postBatch(batch) {
    const response = await fetchImpl(`${sinkUrl}/sink`, {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: JSON.stringify({batchId: batch.batchId, records: batch.records}),
      signal: AbortSignal.any([AbortSignal.timeout(commitTimeoutMs), pipelineController.signal]),
    });
    if (response.status === 422 || response.status === 400) {
      throw new BatchRejected(`下游拒收(${response.status}): ${await response.text()}`);
    }
    if (response.status === 409) throw new BatchConflict('下游存在同身份不同内容的批次');
    if (!response.ok) throw new Error(`下游响应状态 ${response.status}`);
    return response.json();
  }

  async function sinkUnreachable(batch) {
    emit({type: 'sinkUnreachable', batchId: batch.batchId, pending: outbox.counts().pending});
    state.sinkDown = true;
    // 下游失联:停止继续领取,队列中的批次留在发件箱,等待恢复后继续处理。
    for (const worker of workerControllers.values()) {
      worker.controller.abort(new PipelineClosed('下游失联,停止领取'));
    }
    for (const job of state.commitQueue.splice(0)) releaseJobBudget(job);
    checkFinished();
  }

  async function commitJob(job) {
    const {batch} = job;
    if (!job.held) {
      // 恢复重驱:补领预算。允许超限(force),配置缩小也不让恢复卡死,超额计入 overdrawn。
      await acquireDim('pendingRecords', batch.recordCount, {owner: batch.source, force: true});
      await acquireDim('pendingBytes', batch.bytes, {owner: batch.source, force: true});
      job.held = true;
    }
    let terminal = false;
    try {
      let verifyFirst = job.recovered || (batch.attempts ?? 0) > 0;
      let retries = batch.attempts ?? 0;
      while (true) {
        if (state.abortCommitter) return; // 取消:批次保持待提交
        if (verifyFirst) {
          const verdict = await verifyBatch(batch);
          if (verdict === 'done') {
            await confirmBatch(job, 'verified');
            terminal = true;
            return;
          }
          if (verdict === 'conflict') {
            counters.conflicts += 1;
            emit({type: 'batchConflict', batchId: batch.batchId, source: batch.source});
            await failBatch(job, '下游存在同身份不同内容的批次');
            terminal = true;
            return;
          }
          if (verdict === 'unknown') {
            retries += 1;
            batch.attempts = retries;
            counters.retries += 1;
            if (retries > commitRetries) {
              await sinkUnreachable(batch);
              return;
            }
            emit({type: 'commitRetry', batchId: batch.batchId, attempt: retries, reason: '查证失败'});
            await delay(commitBackoffMs * retries, undefined, {signal: pipelineController.signal}).catch(() => {});
            continue;
          }
          // absent:下游确认没有该批,可以安全重发
        }
        try {
          const receipt = await postBatch(batch);
          await confirmBatch(job, 'committed', receipt);
          terminal = true;
          return;
        } catch (error) {
          if (error instanceof BatchRejected) {
            counters.rejected += 1;
            emit({type: 'batchRejected', batchId: batch.batchId, source: batch.source, reason: error.message});
            await failBatch(job, error.message);
            terminal = true;
            return;
          }
          if (error instanceof BatchConflict) {
            counters.conflicts += 1;
            emit({type: 'batchConflict', batchId: batch.batchId, source: batch.source});
            await failBatch(job, '提交冲突');
            terminal = true;
            return;
          }
          // 网络错误/超时/5xx:是否已被接受未知,下次重发前先查证。
          retries += 1;
          batch.attempts = retries;
          counters.retries += 1;
          if (retries > commitRetries) {
            await sinkUnreachable(batch);
            return;
          }
          emit({type: 'commitRetry', batchId: batch.batchId, attempt: retries, reason: String(error?.message ?? error)});
          await delay(commitBackoffMs * retries, undefined, {signal: pipelineController.signal}).catch(() => {});
          verifyFirst = true;
        }
      }
    } finally {
      if (!terminal) releaseJobBudget(job);
    }
  }

  async function commitLoop() {
    while (!state.abortCommitter) {
      const job = state.commitQueue.shift();
      if (!job) {
        await waitCommitter();
        continue;
      }
      state.inflight += 1;
      try {
        await commitJob(job);
      } catch (error) {
        emit({type: 'commitError', batchId: job.batch.batchId, error: String(error?.message ?? error)});
      } finally {
        state.inflight -= 1;
        checkFinished();
      }
    }
    // 退出时释放队列中剩余批次占用的预算;批次本身留在发件箱等待恢复。
    for (const job of state.commitQueue.splice(0)) releaseJobBudget(job);
  }

  function checkFinished() {
    if (state.finished) return;
    const drained = state.commitQueue.length === 0 && state.inflight === 0;
    if (state.sinkDown) {
      if (drained) finish('incomplete');
      return;
    }
    if (state.closing) {
      if (state.sourcesSettled && drained) finish('closed');
      return;
    }
    if (state.sourcesSettled && drained) finish('complete');
  }

  function finish(status) {
    if (state.finished) return;
    state.finished = true;
    state.abortCommitter = true;
    wakeCommitter();
    state.finishResolve(buildReport(status));
  }

  function buildReport(status) {
    return {
      status,
      durationMs: Date.now() - state.startedAt,
      sources: Object.fromEntries([...infos].map(([name, info]) => [name, {...info, waitingOn: null}])),
      commits: {...counters},
      outbox: outbox?.counts() ?? null,
      budget: budget.snapshot(),
    };
  }

  async function run() {
    if (state.started) throw new Error('采集链路只能启动一次');
    if (state.aborted) throw new Error('采集链路已中止,不能启动');
    state.started = true;
    state.startedAt = Date.now();
    outbox = await Outbox.load(outboxPath);
    const recoveredPending = outbox.pending();
    if (recoveredPending.length > 0 || Object.keys(outbox.checkpoints()).length > 0) {
      emit({type: 'recovered', pending: recoveredPending.length, checkpoints: outbox.checkpoints()});
    }
    for (const batch of recoveredPending) {
      state.commitQueue.push({batch, held: false, recovered: true});
    }
    const committer = commitLoop();
    state.committerDone = committer;
    const workers = sources.map(source => runSource(source));
    state.workersSettled = Promise.allSettled(workers).then(() => {
      state.sourcesSettled = true;
      wakeCommitter();
      checkFinished();
    });
    const report = await state.finishedPromise;
    await state.workersSettled;
    await committer;
    return report;
  }

  // 优雅关闭:停止继续领取,来源部分批次入库,收拢仍可核对的提交。
  async function close({drainMs = 8000} = {}) {
    if (!state.started) throw new Error('采集链路尚未启动');
    if (state.finished) return state.finishedPromise;
    if (!state.closing) {
      state.closing = true;
      emit({type: 'closing'});
      for (const worker of workerControllers.values()) {
        worker.controller.abort(new PipelineClosed('采集链路关闭,停止领取'));
      }
      await state.workersSettled;
      const deadline = Date.now() + drainMs;
      while (!state.finished && Date.now() < deadline) await delay(25);
      if (!state.finished) finish('closed');
    }
    return state.finishedPromise;
  }

  // 硬中止:模拟进程被杀。不等待收拢,状态以发件箱为准。
  function abort() {
    if (state.finished) return;
    state.aborted = true;
    pipelineController.abort();
    for (const worker of workerControllers.values()) {
      worker.controller.abort(new PipelineClosed('硬中止'));
    }
    finish('aborted');
  }

  function stats() {
    return {
      state: state.finished
        ? 'finished'
        : state.closing
          ? 'closing'
          : state.sinkDown
            ? 'sink-down'
            : state.started
              ? 'running'
              : 'idle',
      budget: budget.snapshot(),
      sources: Object.fromEntries([...infos].map(([name, info]) => [name, {...info}])),
      commits: {
        queued: state.commitQueue.length,
        inflight: state.inflight,
        sinkDown: state.sinkDown,
        ...counters,
      },
      outbox: outbox ? outbox.counts() : {pending: 0, done: 0, failed: 0},
    };
  }

  return {
    run,
    close,
    abort,
    stats,
    budget,
    dispose: async () => {
      await outbox?.close();
    },
  };
}
