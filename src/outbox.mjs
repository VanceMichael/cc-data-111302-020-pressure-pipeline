// 提交发件箱:把"输入范围 → 下游提交身份"的对应持久化到 JSONL 文件。
// 每条批次记录携带来源名、解压流内的字节区间 [firstOffset, lastOffset)、
// 记录数、字节数、内容指纹与确定性的批次身份(batchId)。
// 崩溃后重放文件即可恢复:
//   done   —— 已确认,不重做;
//   pending —— 未确认,先查证(GET /sink/:batchId)再决定是否重发;
//   failed —— 被拒等终态,保留现场不自动重试。
// 崩溃撕裂的半行按未发生处理,其记录会因断点未覆盖而被重新采集。

import {open, readFile, writeFile, mkdir} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {dirname} from 'node:path';

export function fingerprintOf(records) {
  return createHash('sha256').update(JSON.stringify(records)).digest('hex');
}

// 批次身份对同样的来源、区间与内容是确定的:重启后对同一批工作得到同一身份,
// 下游按身份去重,重发安全。
export function batchIdOf(source, firstOffset, lastOffset, fingerprint) {
  const digest = createHash('sha256')
    .update(`${source}|${firstOffset}|${lastOffset}|${fingerprint}`)
    .digest('hex')
    .slice(0, 24);
  return `b-${digest}`;
}

export class Outbox {
  #path;
  #handle = null;
  #batches = new Map();   // batchId -> {status, batch, receipt, reason}
  #checkpoints = new Map(); // source -> offset(由已压缩掉的 done/failed 批次汇总)
  #doneCount = 0;

  constructor(path) {
    this.#path = path;
  }

  static async load(path) {
    const outbox = new Outbox(path);
    await mkdir(dirname(path), {recursive: true});
    let text = '';
    try {
      text = await readFile(path, 'utf8');
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    for (const line of text.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      let entry;
      try {
        entry = JSON.parse(trimmed);
      } catch {
        continue; // 崩溃撕裂的半行按未发生处理
      }
      outbox.#replay(entry);
    }
    await outbox.#compact();
    outbox.#handle = await open(path, 'a');
    return outbox;
  }

  #replay(entry) {
    if (entry.type === 'checkpoint' && typeof entry.source === 'string') {
      const current = this.#checkpoints.get(entry.source) ?? 0;
      if (entry.offset > current) this.#checkpoints.set(entry.source, entry.offset);
      return;
    }
    if (entry.type === 'batch' && entry.batchId) {
      this.#batches.set(entry.batchId, {status: 'pending', batch: entry});
      return;
    }
    if (entry.type === 'done' && entry.batchId) {
      const current = this.#batches.get(entry.batchId);
      if (current) {
        current.status = 'done';
        current.receipt = entry.receipt ?? null;
        this.#doneCount += 1;
      }
      return;
    }
    if (entry.type === 'fail' && entry.batchId) {
      const current = this.#batches.get(entry.batchId);
      if (current) {
        current.status = 'failed';
        current.reason = entry.reason ?? '未知原因';
      }
    }
  }

  // 压缩:已确认批次折叠成每来源一条断点线,待提交与失败批次原样保留。
  async #compact() {
    const lines = [];
    const checkpoints = new Map(this.#checkpoints);
    for (const {status, batch} of this.#batches.values()) {
      if (status === 'done' || status === 'failed') {
        const current = checkpoints.get(batch.source) ?? 0;
        if (batch.lastOffset > current) checkpoints.set(batch.source, batch.lastOffset);
      }
    }
    for (const [source, offset] of checkpoints) {
      lines.push(JSON.stringify({type: 'checkpoint', source, offset}));
    }
    for (const {status, batch, reason} of this.#batches.values()) {
      if (status === 'done') continue;
      lines.push(JSON.stringify(batch));
      if (status === 'failed') lines.push(JSON.stringify({type: 'fail', batchId: batch.batchId, reason}));
    }
    this.#checkpoints = checkpoints;
    await writeFile(this.#path, lines.length > 0 ? lines.join('\n') + '\n' : '');
  }

  async #append(entry) {
    await this.#handle.appendFile(JSON.stringify(entry) + '\n');
    await this.#handle.sync(); // 落盘后再推进内存状态,保证崩溃可重放
  }

  // 暂存一批:先持久化,再允许进入提交流程。
  async stage(batch) {
    await this.#append(batch);
    this.#batches.set(batch.batchId, {status: 'pending', batch});
  }

  async markDone(batchId, receipt) {
    const current = this.#batches.get(batchId);
    if (!current || current.status !== 'pending') return;
    await this.#append({type: 'done', batchId, receipt: receipt ?? null});
    current.status = 'done';
    current.receipt = receipt ?? null;
    this.#doneCount += 1;
  }

  async markFailed(batchId, reason) {
    const current = this.#batches.get(batchId);
    if (!current || current.status !== 'pending') return;
    await this.#append({type: 'fail', batchId, reason});
    current.status = 'failed';
    current.reason = reason;
  }

  pending() {
    return [...this.#batches.values()].filter(e => e.status === 'pending').map(e => e.batch);
  }

  // 恢复断点:已确认、待提交、已失败的批次覆盖的区间都不需要重新采集。
  checkpointFor(source) {
    let offset = this.#checkpoints.get(source) ?? 0;
    for (const {batch} of this.#batches.values()) {
      if (batch.source === source && batch.lastOffset > offset) offset = batch.lastOffset;
    }
    return offset;
  }

  checkpoints() {
    return Object.fromEntries(this.#checkpoints);
  }

  counts() {
    let pending = 0;
    let failed = 0;
    for (const {status} of this.#batches.values()) {
      if (status === 'pending') pending += 1;
      else if (status === 'failed') failed += 1;
    }
    return {pending, done: this.#doneCount, failed};
  }

  async close() {
    await this.#handle?.close();
    this.#handle = null;
  }
}
