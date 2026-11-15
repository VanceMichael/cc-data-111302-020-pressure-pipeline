// 采集链路演示入口:两个节奏不同的本地限速来源 + 一个慢速接收器,
// 预算故意收紧,运行期间周期性输出各段积压与暂停原因(JSON 行)。
//
//   npm run collect
//
// 可用环境变量调整预算与行为(默认值就是为了让预算真的收紧):
//   RAW_BYTES=8192 INFLATED_BYTES=49152 PENDING_RECORDS=120 PENDING_BYTES=196608 CONNECTIONS=4
//   BATCH_RECORDS=40 BATCH_BYTES=65536 MAX_RECORD_BYTES=131072 SINK_DELAY=120 STATS_MS=1000
//   OUTBOX=.local/collect-outbox.jsonl
//
// 运行中按一次 Ctrl+C 优雅关闭(停止领取、收拢可核对的提交),再按一次硬中止。
// 发件箱保留在 .local/,再次启动会演示恢复:已确认的不重做,未知提交先查证。

import {mkdir} from 'node:fs/promises';
import {createSource} from './source.mjs';
import {createPipeline} from './pipeline.mjs';
import {ResourceBudget} from './budget.mjs';

const env = process.env;
const int = (name, fallback) => {
  const value = Number.parseInt(env[name] ?? '', 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
};

const budget = new ResourceBudget({
  rawBytes: int('RAW_BYTES', 4 * 1024),
  inflatedBytes: int('INFLATED_BYTES', 48 * 1024),
  pendingRecords: int('PENDING_RECORDS', 120),
  pendingBytes: int('PENDING_BYTES', 192 * 1024),
  connections: int('CONNECTIONS', 4),
});

const outboxPath = env.OUTBOX ?? '.local/collect-outbox.jsonl';
await mkdir('.local', {recursive: true});

// 来源 A:慢速分块,且混有一条 300KB 的超大记录(超过 MAX_RECORD_BYTES,走独立策略)。
const sizesA = Array.from({length: 400}, () => 2000);
sizesA[137] = 300_000;
const sourceA = await createSource();
Object.assign(sourceA.state, {recordSizes: sizesA, chunkBytes: 4096, delay: 8, noise: true});
// 来源 B:稳定流速,低可压缩内容且瞬时倾泻,与 A 竞争原始字节预算。
const sourceB = await createSource();
Object.assign(sourceB.state, {count: 1500, recordBytes: 300, chunkBytes: 8192, delay: 0, noise: true});
// 下游:慢速接收器。
const sink = await createSource();
Object.assign(sink.state, {sinkDelay: int('SINK_DELAY', 120)});

const pipeline = createPipeline({
  sources: [
    {name: 'a-慢速分块', url: sourceA.url, path: '/stream'},
    {name: 'b-稳定流', url: sourceB.url, path: '/stream'},
  ],
  sinkUrl: sink.url,
  outboxPath,
  budget,
  batchRecords: int('BATCH_RECORDS', 40),
  batchBytes: int('BATCH_BYTES', 64 * 1024),
  maxRecordBytes: int('MAX_RECORD_BYTES', 128 * 1024),
  oversized: 'skip',
  commitRetries: 3,
  onEvent: event => console.log(JSON.stringify({type: 'event', ...event})),
});

console.log(
  JSON.stringify({
    type: 'start',
    sources: {a: sourceA.url, b: sourceB.url},
    sink: sink.url,
    budget: budget.limits(),
    outbox: outboxPath,
  }),
);

function compactStats(stats) {
  const budgetView = {};
  for (const [dim, d] of Object.entries(stats.budget.dimensions)) {
    budgetView[dim] = {used: d.used, limit: d.limit, waiting: d.waiting, waits: d.waits, peak: d.peak};
  }
  const sourcesView = {};
  for (const [name, info] of Object.entries(stats.sources)) {
    sourcesView[name] = {
      status: info.status,
      parsed: info.parsed,
      committed: info.committedRecords,
      waitingOn: info.waitingOn ? `${info.waitingOn.dim}(${Date.now() - info.waitingOn.since}ms)` : null,
    };
  }
  return {budget: budgetView, sources: sourcesView, commits: stats.commits, outbox: stats.outbox};
}

const statsMs = int('STATS_MS', 1000);
const timer = setInterval(() => {
  console.log(JSON.stringify({type: 'stats', ...compactStats(pipeline.stats())}));
}, statsMs);

let closing = false;
process.on('SIGINT', () => {
  if (!closing) {
    closing = true;
    console.log(JSON.stringify({type: 'closing', message: '收到 SIGINT,停止领取并收拢可核对的提交'}));
    pipeline.close();
  } else {
    console.log(JSON.stringify({type: 'aborting', message: '再次 SIGINT,硬中止'}));
    pipeline.abort();
  }
});

const report = await pipeline.run();
clearInterval(timer);
console.log(
  JSON.stringify({
    type: 'report',
    status: report.status,
    durationMs: report.durationMs,
    sources: report.sources,
    commits: report.commits,
    outbox: report.outbox,
    budgetPeaks: Object.fromEntries(
      Object.entries(report.budget.dimensions).map(([dim, d]) => [
        dim,
        {peak: d.peak, limit: d.limit, waits: d.waits, peakWaiters: d.peakWaiters},
      ]),
    ),
  }),
);
await pipeline.dispose();
await sourceA.close();
await sourceB.close();
await sink.close();
process.exitCode = report.status === 'complete' ? 0 : 1;
