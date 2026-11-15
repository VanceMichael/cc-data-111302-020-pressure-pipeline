// 增量 NDJSON 解析:输入解压后的字节块,逐条产出记录及其在解压流中的字节区间
// (start/end 为解压后的字节偏移,end 不含下一条记录,用于恢复时的断点跳过)。
// 单条记录超过 maxRecordBytes 时按策略处理:
//   skip —— 丢弃该条并报告真实字节数,流继续;
//   fail —— 抛出 RecordTooLarge,由来源级错误处理决定去向。
// 任何情况下都不截断记录,也不因为一条坏记录把整个流当坏数据丢掉。

export class RecordTooLarge extends Error {
  constructor(size, limit) {
    super(`单条记录达到 ${size} 字节,超过上限 ${limit}`);
    this.name = 'RecordTooLarge';
    this.size = size;
    this.limit = limit;
  }
}

const LF = 0x0a;
const CR = 0x0d;

export async function* parseNdjson(chunks, {maxRecordBytes, oversized = 'fail', onOversized} = {}) {
  const iterator = chunks[Symbol.asyncIterator]();
  let parts = [];        // 当前记录的字节片段
  let size = 0;          // 当前记录已累积字节(不含换行符)
  let head = 0;          // 当前记录起始的解压偏移
  let consumed = 0;      // 已消费的解压字节总数
  let skipping = false;  // 正在跳过一条超大记录
  let skipFrom = 0;      // 超大记录的起始偏移
  try {
    while (true) {
      const {value: chunk, done} = await iterator.next();
      if (done) break;
      const base = consumed;
      consumed += chunk.length;
      let from = 0;
      while (from < chunk.length) {
        if (skipping) {
          const nl = chunk.indexOf(LF, from);
          if (nl === -1) {
            from = chunk.length;
            break;
          }
          const end = base + nl + 1;
          skipping = false;
          await onOversized?.({start: skipFrom, end, bytes: end - skipFrom, policy: 'skip'});
          head = end;
          from = nl + 1;
          continue;
        }
        const nl = chunk.indexOf(LF, from);
        const take = nl === -1 ? chunk.length : nl; // 记录内容在本块的结束位置(不含换行符)
        if (maxRecordBytes !== undefined && size + (take - from) > maxRecordBytes) {
          if (oversized === 'fail') throw new RecordTooLarge(size + take - from, maxRecordBytes);
          skipping = true;
          skipFrom = head;
          parts = [];
          size = 0;
          continue; // 不前进 from,交给 skipping 分支继续扫描换行符
        }
        if (take > from) {
          parts.push(chunk.subarray(from, take));
          size += take - from;
        }
        if (nl === -1) {
          from = chunk.length;
          break;
        }
        const start = head;
        const end = base + nl + 1;
        let body = parts.length === 1 ? parts[0] : Buffer.concat(parts, size);
        parts = [];
        size = 0;
        head = end;
        from = nl + 1;
        if (body.length === 0) continue; // 空行:偏移照算,记录不产出
        if (body[body.length - 1] === CR) body = body.subarray(0, body.length - 1);
        if (body.length === 0) continue;
        yield {body, start, end};
      }
    }
    if (skipping) {
      await onOversized?.({start: skipFrom, end: consumed, bytes: consumed - skipFrom, policy: 'skip'});
      return;
    }
    if (size > 0) {
      // 末尾没有换行符的残留内容作为最后一条记录
      const body = parts.length === 1 ? parts[0] : Buffer.concat(parts, size);
      yield {body, start: head, end: consumed};
    }
  } finally {
    await iterator.return?.();
  }
}
