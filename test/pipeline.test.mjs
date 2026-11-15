import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createSource} from '../src/source.mjs';
import {ResourceBudget} from '../src/budget.mjs';
import {parseNdjson, RecordTooLarge} from '../src/ndjson.mjs';
import {createPipeline} from '../src/pipeline.mjs';

// ---------- 工具 ----------

async function* toAsync(chunks) {
  yield* chunks;
}

function chunkify(text, size) {
  const bytes = Buffer.from(text);
  const chunks = [];
  for (let i = 0; i < bytes.length; i += size) chunks.push(bytes.subarray(i, i + size));
  return chunks;
}

async function waitFor(condition, {timeout = 5000, interval = 5} = {}) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (condition()) return;
    await delay(interval);
  }
  throw new Error('等待条件超时');
}

function committedRecords(sink) {
  return Object.values(sink.state.accepted).reduce((sum, receipt) => sum + receipt.count, 0);
}

function countingFetch(log) {
  return async (url, options = {}) => {
    let batchId = null;
    try {
      batchId = JSON.parse(options.body)?.batchId ?? null;
    } catch {}
    log.push({method: options.method ?? 'GET', url: String(url), batchId});
    return fetch(url, options);
  };
}

// 组装一套来源 + 下游 + 管线;预算与管线参数按测试需要显式收紧。
async function makeHarness({sourceProfiles = [{}], sinkProfile = {}, budgets, pipeline: pipelineOptions = {}} = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'pressure-pipeline-'));
  const servers = [];
  const sources = [];
  for (let i = 0; i < sourceProfiles.length; i += 1) {
    const server = await createSource();
    Object.assign(server.state, sourceProfiles[i]);
    servers.push(server);
    sources.push({name: `s${i}`, url: server.url, path: '/stream'});
  }
  const sink = await createSource();
  Object.assign(sink.state, sinkProfile);
  servers.push(sink);
  const events = [];
  const fetches = [];
  const pipeline = createPipeline({
    sources,
    sinkUrl: sink.url,
    outboxPath: join(dir, 'outbox.jsonl'),
    budget: new ResourceBudget(budgets),
    fetchImpl: countingFetch(fetches),
    onEvent: event => events.push(event),
    batchRecords: 10,
    batchBytes: 64 * 1024,
    maxRecordBytes: 256 * 1024,
    commitTimeoutMs: 1500,
    commitBackoffMs: 50,
    ...pipelineOptions,
  });
  const cleanup = async () => {
    await pipeline.dispose();
    await Promise.all(servers.map(server => server.close()));
    await rm(dir, {recursive: true, force: true});
  };
  return {dir, sources, sink, servers, events, fetches, pipeline, cleanup};
}

// ---------- 预算与解析器单元行为 ----------

test('资源预算:限额、排队、公平授予与取消', async () => {
  const budget = new ResourceBudget({bytes: 10, connections: 1});
  await budget.acquire('bytes', 7, {owner: 'a'});
  assert.equal(budget.tryAcquire('bytes', 5, 'b'), false);
  const order = [];
  const w1 = budget.acquire('bytes', 4, {owner: 'w1'}).then(() => order.push('w1'));
  const w2 = budget.acquire('bytes', 2, {owner: 'w2'}).then(() => order.push('w2'));
  await delay(10);
  budget.release('bytes', 7, 'a');
  await Promise.all([w1, w2]);
  assert.deepEqual(order, ['w1', 'w2'], '等待者应按到达顺序获得预算');
  await assert.rejects(budget.acquire('bytes', 11), /超过上限/);
  await budget.acquire('connections', 1, {owner: 'holder'});
  const controller = new AbortController();
  const waiting = budget.acquire('connections', 1, {owner: 'late', signal: controller.signal});
  controller.abort(new Error('取消'));
  await assert.rejects(waiting, /取消/);
  const snap = budget.snapshot();
  assert.ok(snap.dimensions.bytes.waits >= 2);
  assert.equal(snap.dimensions.bytes.used, 6);
  budget.release('bytes', 4, 'w1');
  budget.release('bytes', 2, 'w2');
  budget.release('connections', 1, 'holder');
  const after = budget.snapshot();
  assert.equal(after.dimensions.bytes.used, 0);
  assert.equal(after.dimensions.connections.used, 0);
  assert.deepEqual(after.owners, {});
});

test('NDJSON 解析:偏移、跨块、空行与无换行尾行', async () => {
  const text = '{"a":1}\n{"b":"跨块记录"}\n\n{"c":3}';
  const records = [];
  for await (const record of parseNdjson(toAsync(chunkify(text, 7)), {maxRecordBytes: 1024})) {
    records.push(record);
  }
  assert.equal(records.length, 3);
  assert.equal(records[0].body.toString(), '{"a":1}');
  assert.equal(records[0].start, 0);
  assert.equal(records[0].end, Buffer.from('{"a":1}\n').length);
  assert.equal(records[1].body.toString(), '{"b":"跨块记录"}');
  assert.equal(records[1].start, records[0].end);
  assert.equal(records[2].body.toString(), '{"c":3}');
  assert.equal(records[2].end, Buffer.from(text).length, '尾行没有换行符也要产出且偏移到流末尾');
});

test('超大记录:skip 报告真实字节数并继续,fail 抛错且不丢整流语义', async () => {
  const text = `{"id":1}\n{"big":"${'x'.repeat(1000)}"}\n{"id":2}\n`;
  const reports = [];
  const records = [];
  for await (const record of parseNdjson(toAsync(chunkify(text, 100)), {
    maxRecordBytes: 100,
    oversized: 'skip',
    onOversized: async report => reports.push(report),
  })) {
    records.push(JSON.parse(record.body.toString()));
  }
  assert.deepEqual(records.map(r => r.id), [1, 2], '超大记录前后的记录必须保留');
  assert.equal(reports.length, 1);
  assert.ok(reports[0].bytes > 1000, `报告的字节数被截断: ${reports[0].bytes}`);
  assert.ok(reports[0].end > reports[0].start);
  await assert.rejects(
    async () => {
      for await (const record of parseNdjson(toAsync(chunkify(text, 100)), {maxRecordBytes: 100, oversized: 'fail'})) {
        void record;
      }
    },
    RecordTooLarge,
  );
});

// ---------- 管线集成:预算真的约束生产路径 ----------

test('字节预算在并发与压缩膨胀下约束生产路径', async () => {
  const h = await makeHarness({
    // 数据量(~1.6MB 解压)远大于各环节内部缓冲,预算必然成为约束而不是摆设
    sourceProfiles: [
      {count: 1500, recordBytes: 512, chunkBytes: 16384, delay: 0, noise: true},
      {count: 1500, recordBytes: 512, chunkBytes: 16384, delay: 0, noise: true},
    ],
    // 慢接收器让待提交预算先满,压力沿 解析→解压→原始字节 逐级回传
    sinkProfile: {sinkDelay: 20},
    budgets: {rawBytes: 4 * 1024, inflatedBytes: 24 * 1024, pendingRecords: 120, pendingBytes: 1024 * 1024, connections: 4},
    pipeline: {batchRecords: 20},
  });
  try {
    const report = await h.pipeline.run();
    assert.equal(report.status, 'complete');
    assert.equal(committedRecords(h.sink), 3000);
    const dims = report.budget.dimensions;
    assert.ok(dims.rawBytes.peak <= 4 * 1024, `rawBytes 峰值越界: ${dims.rawBytes.peak}`);
    assert.ok(dims.inflatedBytes.peak <= 24 * 1024, `inflatedBytes 峰值越界: ${dims.inflatedBytes.peak}`);
    assert.ok(dims.pendingRecords.peak <= 120, `pendingRecords 峰值越界: ${dims.pendingRecords.peak}`);
    assert.ok(dims.rawBytes.waits > 0, '原始字节预算从未收紧,证明无效');
    assert.ok(dims.inflatedBytes.waits > 0, '解压字节预算从未收紧,证明无效');
    assert.ok(dims.pendingRecords.waits > 0, '待提交记录预算从未收紧,证明无效');
    // 处理总量远超预算:预算是反复约束生产路径,而不是数据调小后自然装得下
    const rawTotal = report.sources.s0.bytesRaw + report.sources.s1.bytesRaw;
    const inflatedTotal = report.sources.s0.bytesInflated + report.sources.s1.bytesInflated;
    assert.ok(rawTotal > 8 * 4 * 1024, `原始字节总量太小: ${rawTotal}`);
    assert.ok(inflatedTotal > 5 * 24 * 1024, `解压字节总量太小: ${inflatedTotal}`);
    for (const [dim, d] of Object.entries(dims)) {
      assert.equal(d.used, 0, `${dim} 预算泄漏`);
      assert.equal(d.anomalies, 0, `${dim} 出现超额释放`);
    }
    assert.deepEqual(report.budget.owners, {});
  } finally {
    await h.cleanup();
  }
});

test('慢速接收器触发待提交背压,暂停原因可观察', async () => {
  const h = await makeHarness({
    sourceProfiles: [{count: 120, recordBytes: 200, chunkBytes: 1024, delay: 0}],
    sinkProfile: {sinkDelay: 100},
    budgets: {rawBytes: 64 * 1024, inflatedBytes: 64 * 1024, pendingRecords: 30, pendingBytes: 256 * 1024, connections: 2},
    pipeline: {batchRecords: 10},
  });
  try {
    let sawPausedOnPending = false;
    const sampler = setInterval(() => {
      const stats = h.pipeline.stats();
      if (stats.sources.s0?.waitingOn?.dim === 'pendingRecords') sawPausedOnPending = true;
    }, 5);
    const report = await h.pipeline.run();
    clearInterval(sampler);
    assert.equal(report.status, 'complete');
    assert.equal(committedRecords(h.sink), 120);
    const dims = report.budget.dimensions;
    assert.ok(dims.pendingRecords.peak <= 30, `pendingRecords 峰值越界: ${dims.pendingRecords.peak}`);
    assert.ok(dims.pendingRecords.waits > 0, '待提交记录预算从未收紧');
    assert.ok(sawPausedOnPending, '未观察到来源因待提交记录而暂停');
  } finally {
    await h.cleanup();
  }
});

test('一条慢流不会永久拖住其他来源', async () => {
  const h = await makeHarness({
    sourceProfiles: [
      {count: 30, recordBytes: 100, chunkBytes: 128, delay: 30, noise: true},
      {count: 80, recordBytes: 100, chunkBytes: 4096, delay: 0},
    ],
    budgets: {rawBytes: 64 * 1024, inflatedBytes: 64 * 1024, pendingRecords: 200, pendingBytes: 256 * 1024, connections: 4},
  });
  try {
    const report = await h.pipeline.run();
    assert.equal(report.status, 'complete');
    const fastDone = h.events.findIndex(e => e.type === 'sourceDone' && e.source === 's1');
    const slowDone = h.events.findIndex(e => e.type === 'sourceDone' && e.source === 's0');
    assert.ok(fastDone !== -1 && slowDone !== -1, '两个来源都应完成');
    assert.ok(fastDone < slowDone, '快来源必须先于慢来源完成,不被拖住');
    assert.equal(committedRecords(h.sink), 110);
  } finally {
    await h.cleanup();
  }
});

test('超大单条记录:报告、不截断、不丢整流', async () => {
  const h = await makeHarness({
    sourceProfiles: [{recordSizes: [64, 64, 300_000, 64, 64], chunkBytes: 4096, delay: 0}],
    budgets: {rawBytes: 64 * 1024, inflatedBytes: 64 * 1024, pendingRecords: 50, pendingBytes: 256 * 1024, connections: 2},
    pipeline: {maxRecordBytes: 4096, oversized: 'skip'},
  });
  try {
    const report = await h.pipeline.run();
    assert.equal(report.status, 'complete');
    assert.equal(committedRecords(h.sink), 4, '超大记录之外的 4 条必须全部落库');
    const oversized = h.events.filter(e => e.type === 'recordOversized');
    assert.equal(oversized.length, 1);
    assert.ok(oversized[0].bytes >= 300_000, `报告的字节数被截断: ${oversized[0].bytes}`);
    assert.equal(report.sources.s0.oversized, 1);
    assert.equal(report.sources.s0.parsed, 4);
  } finally {
    await h.cleanup();
  }
});

test('超大记录 fail 策略:该来源报错,其他来源继续', async () => {
  const h = await makeHarness({
    sourceProfiles: [
      {recordSizes: [64, 300_000, 64], chunkBytes: 4096},
      {count: 20, recordBytes: 100, chunkBytes: 1024},
    ],
    budgets: {rawBytes: 64 * 1024, inflatedBytes: 64 * 1024, pendingRecords: 50, pendingBytes: 256 * 1024, connections: 2},
    pipeline: {maxRecordBytes: 4096, oversized: 'fail'},
  });
  try {
    const report = await h.pipeline.run();
    assert.equal(report.status, 'complete', '单个来源失败不阻塞整体完成');
    assert.equal(report.sources.s0.status, 'error');
    assert.match(report.sources.s0.error, /超过上限/);
    assert.equal(report.sources.s1.status, 'done');
    assert.equal(committedRecords(h.sink), 21, 's0 出错前的 1 条与 s1 的 20 条应落库');
    assert.equal(h.events.filter(e => e.type === 'sourceError').length, 1);
  } finally {
    await h.cleanup();
  }
});

// ---------- 恢复、取消与下游故障 ----------

test('硬中止后恢复:只重做未确认的工作,未知提交先查证', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'pressure-pipeline-'));
  const outboxPath = join(dir, 'outbox.jsonl');
  const sourceServer = await createSource();
  Object.assign(sourceServer.state, {count: 60, recordBytes: 150, chunkBytes: 1024, delay: 0});
  const sink = await createSource();
  Object.assign(sink.state, {dropAck: true});
  const budgets = () => ({rawBytes: 64 * 1024, inflatedBytes: 64 * 1024, pendingRecords: 100, pendingBytes: 256 * 1024, connections: 2});
  const events1 = [];
  const fetches1 = [];
  const pipeline1 = createPipeline({
    sources: [{name: 's0', url: sourceServer.url, path: '/stream'}],
    sinkUrl: sink.url,
    outboxPath,
    budget: new ResourceBudget(budgets()),
    fetchImpl: countingFetch(fetches1),
    onEvent: event => events1.push(event),
    batchRecords: 10,
    maxRecordBytes: 256 * 1024,
    commitBackoffMs: 500,
    commitRetries: 3,
  });
  try {
    const run1 = pipeline1.run();
    // 等到第一批被下游实际接受但确认丢失(dropAck),然后硬中止,模拟进程被杀
    await waitFor(() => Object.keys(sink.state.accepted).length >= 1 && events1.some(e => e.type === 'commitRetry'));
    pipeline1.abort();
    const report1 = await run1;
    assert.equal(report1.status, 'aborted');
    const acceptedBeforeCrash = Object.keys(sink.state.accepted);
    assert.ok(acceptedBeforeCrash.length >= 1, '崩溃前下游应已接受至少一批');
    assert.ok(report1.outbox.pending >= 1, '崩溃后发件箱应留有未确认批次');
    await pipeline1.dispose();
    sink.state.dropAck = false;

    // 第二次运行:同一发件箱,恢复
    const events2 = [];
    const fetches2 = [];
    const pipeline2 = createPipeline({
      sources: [{name: 's0', url: sourceServer.url, path: '/stream'}],
      sinkUrl: sink.url,
      outboxPath,
      budget: new ResourceBudget(budgets()),
      fetchImpl: countingFetch(fetches2),
      onEvent: event => events2.push(event),
      batchRecords: 10,
      maxRecordBytes: 256 * 1024,
      commitBackoffMs: 50,
      commitRetries: 3,
    });
    const report2 = await pipeline2.run();
    assert.equal(report2.status, 'complete');
    assert.equal(committedRecords(sink), 60, '全部记录恰好落库一次,不重不漏');
    // 恢复时先查证:崩溃前已接受的批次不允许重发
    const posts2 = fetches2.filter(f => f.method === 'POST');
    for (const batchId of acceptedBeforeCrash) {
      assert.ok(!posts2.some(p => p.batchId === batchId), `已接受的批次 ${batchId} 被重发`);
    }
    assert.ok(
      fetches2.some(f => f.method === 'GET' && f.url.includes('/sink/')),
      '恢复路径必须先查证未知提交',
    );
    assert.ok(events2.some(e => e.type === 'batchVerified'), '已接受的未知提交应经查证直接确认');
    assert.equal(
      Object.keys(sink.state.accepted).length,
      acceptedBeforeCrash.length + posts2.length,
      '重发的批次恰好是下游确认没有的部分',
    );
    // 已入库区间不重新采集
    assert.ok(report2.sources.s0.skipped > 0, '恢复后应跳过已入库的记录区间');
    await pipeline2.dispose();
  } finally {
    await pipeline1.dispose();
    await sourceServer.close();
    await sink.close();
    await rm(dir, {recursive: true, force: true});
  }
});

test('优雅关闭:停止领取、收拢提交、预算归零,重启后续跑完成', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'pressure-pipeline-'));
  const outboxPath = join(dir, 'outbox.jsonl');
  const sourceServer = await createSource();
  Object.assign(sourceServer.state, {count: 500, recordBytes: 300, chunkBytes: 512, delay: 5, noise: true});
  const sink = await createSource();
  const budgets = () => ({rawBytes: 32 * 1024, inflatedBytes: 64 * 1024, pendingRecords: 100, pendingBytes: 256 * 1024, connections: 2});
  const make = (events, fetches) =>
    createPipeline({
      sources: [{name: 's0', url: sourceServer.url, path: '/stream'}],
      sinkUrl: sink.url,
      outboxPath,
      budget: new ResourceBudget(budgets()),
      fetchImpl: countingFetch(fetches),
      onEvent: event => events.push(event),
      batchRecords: 20,
      maxRecordBytes: 256 * 1024,
      commitBackoffMs: 50,
    });
  const events1 = [];
  const fetches1 = [];
  const pipeline1 = make(events1, fetches1);
  try {
    const run1 = pipeline1.run();
    await delay(150); // 流式进行中
    const report1 = await pipeline1.close({drainMs: 5000});
    assert.equal(report1.status, 'closed');
    assert.equal(await run1, report1);
    for (const [dim, d] of Object.entries(report1.budget.dimensions)) {
      assert.equal(d.used, 0, `关闭后 ${dim} 预算泄漏`);
    }
    assert.ok(report1.outbox.done + report1.outbox.pending > 0, '关闭前应已有批次入库或提交');
    await pipeline1.dispose();

    const events2 = [];
    const fetches2 = [];
    const pipeline2 = make(events2, fetches2);
    const report2 = await pipeline2.run();
    assert.equal(report2.status, 'complete');
    assert.equal(committedRecords(sink), 500, '关闭 + 重启后全部记录恰好落库一次');
    await pipeline2.dispose();
  } finally {
    await pipeline1.dispose();
    await sourceServer.close();
    await sink.close();
    await rm(dir, {recursive: true, force: true});
  }
});

test('下游整批拒收:记录为失败并保留现场,链路继续处理', async () => {
  const h = await makeHarness({
    sourceProfiles: [{count: 30, recordBytes: 100, chunkBytes: 1024, delay: 0}],
    sinkProfile: {rejectBatch: true},
    budgets: {rawBytes: 64 * 1024, inflatedBytes: 64 * 1024, pendingRecords: 50, pendingBytes: 256 * 1024, connections: 2},
  });
  try {
    const report = await h.pipeline.run();
    assert.equal(report.status, 'complete', '拒收是终态,链路不应卡死');
    assert.equal(committedRecords(h.sink), 0);
    assert.equal(report.outbox.failed, 3, '三批都应记为失败并留在发件箱');
    assert.equal(h.events.filter(e => e.type === 'batchRejected').length, 3);
  } finally {
    await h.cleanup();
  }
});

test('下游失联:保留可继续处理的状态,恢复后完成', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'pressure-pipeline-'));
  const outboxPath = join(dir, 'outbox.jsonl');
  const sourceServer = await createSource();
  Object.assign(sourceServer.state, {count: 30, recordBytes: 100, chunkBytes: 1024, delay: 0});
  const sink = await createSource();
  const sinkPort = new URL(sink.url).port;
  await sink.close(); // 下游失联
  const budgets = () => ({rawBytes: 64 * 1024, inflatedBytes: 64 * 1024, pendingRecords: 50, pendingBytes: 256 * 1024, connections: 2});
  const events1 = [];
  const pipeline1 = createPipeline({
    sources: [{name: 's0', url: sourceServer.url, path: '/stream'}],
    sinkUrl: `http://127.0.0.1:${sinkPort}`,
    outboxPath,
    budget: new ResourceBudget(budgets()),
    onEvent: event => events1.push(event),
    batchRecords: 10,
    maxRecordBytes: 256 * 1024,
    commitRetries: 2,
    commitBackoffMs: 30,
    commitTimeoutMs: 300,
  });
  try {
    const report1 = await pipeline1.run();
    assert.equal(report1.status, 'incomplete');
    assert.ok(events1.some(e => e.type === 'sinkUnreachable'), '应报告下游失联');
    assert.ok(report1.outbox.pending > 0, '未确认批次必须留在发件箱');
    await pipeline1.dispose();

    // 下游恢复,同一发件箱续跑
    const sink2 = await createSource({port: Number(sinkPort)});
    const pipeline2 = createPipeline({
      sources: [{name: 's0', url: sourceServer.url, path: '/stream'}],
      sinkUrl: sink2.url,
      outboxPath,
      budget: new ResourceBudget(budgets()),
      onEvent: () => {},
      batchRecords: 10,
      maxRecordBytes: 256 * 1024,
      commitRetries: 2,
      commitBackoffMs: 30,
    });
    const report2 = await pipeline2.run();
    assert.equal(report2.status, 'complete');
    assert.equal(committedRecords(sink2), 30);
    await pipeline2.dispose();
    await sink2.close();
  } finally {
    await pipeline1.dispose();
    await sourceServer.close();
    await rm(dir, {recursive: true, force: true});
  }
});
