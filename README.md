# 慢速落库下的采集字节背压与可恢复提交

这是用于内部数据接入的 Node.js 项目。当前提供可独立启动的本地来源、单次原文读取接口以及来源契约测试；浏览器页面和仪器网关使用合成业务资料，不连接真实账户或设备。

采集链路（`src/pipeline.mjs`）在字节、解压、解析、提交各段实行可配置的资源预算，并把输入范围到下游提交身份的对应持久化，支持崩溃恢复与优雅取消。设计细节见 `docs/backpressure.md`。

## 环境与命令

Node.js 22.22.1 或更新的 22.x。依赖由 package-lock.json 锁定。

```sh
npm ci --ignore-scripts
npm run build
npm test
npm start
```

`npm start` 在标准输出报告来源地址。默认绑定本机回环地址并使用动态端口；可通过 `HOST` 与 `PORT` 指定监听地址和端口，例如 `HOST=127.0.0.1 PORT=8080 npm start`。按 Ctrl+C 关闭来源；动态端口避免与同机其他接入站冲突。`npm run demo` 在同一进程启动来源，读取一次目录后退出，不代表持续接收业务已经完成。

`npm run collect` 运行采集链路演示：两个节奏不同的本地限速来源（慢速分块且混有超大记录、稳定倾泻）加一个慢速接收器，预算故意收紧，运行期间周期性输出各段积压与暂停原因（JSON 行），结束报告各维度峰值与收紧次数。预算可用环境变量调整：`RAW_BYTES`、`INFLATED_BYTES`、`PENDING_RECORDS`、`PENDING_BYTES`、`CONNECTIONS`、`BATCH_RECORDS`、`BATCH_BYTES`、`MAX_RECORD_BYTES`、`SINK_DELAY`、`STATS_MS`、`OUTBOX`。运行中按一次 Ctrl+C 优雅关闭（停止领取、收拢可核对的提交），再按一次硬中止。发件箱默认写在 `.local/collect-outbox.jsonl`，再次启动会演示恢复：已确认的不重做，未知提交先查证；`rm -rf .local` 可重置演示。

```sh
docker build -t pressure-pipeline:local .
docker run --rm --network none pressure-pipeline:local
docker run --rm --network none pressure-pipeline:local npm run demo
```

依赖在镜像构建阶段安装，运行阶段不需要公网。完整来源测试和一次目录演示通常数秒即可结束，适合在90秒以内查看真实输出。浏览器在无界面模式执行，页面交互不依赖人工鼠标。

## 目录

- `src/source.mjs`：本地来源生命周期与HTTP入口。
- `src/client.mjs`：目录读取及原始字节获取。
- `src/budget.mjs`：多维资源预算（原始字节、解压字节、待提交记录、连接）。
- `src/ndjson.mjs`：增量 NDJSON 解析，超大单条记录的独立策略与报告。
- `src/outbox.mjs`：提交发件箱，输入范围到下游提交身份的持久对应。
- `src/pipeline.mjs`：采集管线，背压贯通、查证重试、取消收拢。
- `src/collect.mjs`：采集链路演示入口，周期性输出各段积压与暂停原因。
- `fixtures/source.json`：来源初始状态。
- `docs/source-contract.md`：来源协议范围、控制与领域语义。
- `docs/backpressure.md`：背压与可恢复提交的设计说明。
- `test/source.test.mjs`：实际来源集成测试。
- `test/pipeline.test.mjs`：背压预算与恢复行为的集成测试。

运行时状态仅用于来源模拟，重启来源会回到 fixtures 中的值。接入业务自己的持久状态请使用 `.local/` 或明确的外部路径。来源管理入口 `/state` 和 `/control` 是本地操作设施，不能作为绕过浏览器页面的业务写入捷径。
