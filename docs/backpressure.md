# 采集链路的背压与可恢复提交

数据接入站一次接几十个授权来源。网络很快,落库端会周期性变慢;只限制请求并发
挡不住内存峰值——两个来源返回压缩后的大对象,解析器又积攒整批记录,进程被杀,
重启还把已写入的数据再写一遍。本文件说明这套机制如何在 字节 → 解压 → 解析 → 提交
全链路上限制在途资源,并在取消、崩溃与下游故障时保持可恢复。

## 资源预算(src/budget.mjs)

五个维度分别计量,而不是只限制请求数:

| 维度 | 约束的在途资源 |
| --- | --- |
| `rawBytes` | 已从 socket 读出、尚未被解压消费的网络字节(gzip 后) |
| `inflatedBytes` | 已解压、尚未被解析消费的字节 |
| `pendingRecords` | 已解析、等待提交确认的记录条数 |
| `pendingBytes` | 待提交记录的字节总量 |
| `connections` | 同时打开的来源连接数 |

每个维度独立排队,先到的等待者先获得(队头阻塞式 FIFO),不会被后来的小请求插队。
`acquire` 支持 `AbortSignal`,取消时等待立即失败。`snapshot()` 给出各维度当前用量、
等待者(物主、需求量、已等待时长)、历史峰值与收紧次数(`waits`)——`waits > 0`
是预算真的约束过生产路径的证据,峰值 `peak ≤ limit` 是上限被守住的证据。

网络块与解压块按预算上限切片领取,因此预算可以小于单个网络块;单批与单条记录
必须能放进待提交预算,管线启动时校验(`batchRecords ≤ pendingRecords`、
`batchBytes ≤ pendingBytes`、`maxRecordBytes ≤ pendingBytes`),不满足直接报错,
不允许运行期卡死。

## 背压如何贯通(src/pipeline.mjs)

每一跳都是拉取式,预算不批就不拉下一块:

```
socket ──rawBytes──▶ gunzip ──inflatedBytes──▶ NDJSON 解析 ──pendingRecords/pendingBytes──▶ 发件箱 ─▶ 提交器
```

- 原始字节:手动把切片写入 gunzip 并等 `drain`,预算持有到 gunzip 真正消费;
  不经过 `Readable.from`/`pipe` 的预读缓冲,预算没有暗渠。socket 侧由 TCP 背压自然限速。
- 解压:gunzip 每个输出块先领预算再交给解析器。gunzip 内部窗口(约 32KB/来源)
  是 zlib 固有缓冲,有界但不计入预算;预算约束的是应用层在途量。
- 解析(src/ndjson.mjs):逐块增量切分,记录携带解压流内的字节区间 `[start, end)`。
  单条超过 `maxRecordBytes` 走独立策略:`skip` 丢弃该条并报告真实字节数(事件
  `recordOversized`),流继续;`fail` 让该来源报错,其他来源继续。任何情况下
  不截断记录,也不把整流当坏数据丢掉。坏 JSON 行报告 `recordInvalid` 后跳过。
- 待提交:记录入批前领取 `pendingRecords`/`pendingBytes`,批次确认后才释放。
  下游变慢时这里先满,压力沿解析→解压→原始字节逐级回传,来源自然暂停。

每个来源一个 worker,互不占线:一条慢流只占用自己的连接与已领预算。来源空闲超过
`sourceIdleTimeoutMs` 没有产出字节会被中止(事件 `sourceTimeout`),不影响其他来源。

## 输入范围到提交身份的对应(src/outbox.mjs)

批次入队前先写发件箱(JSONL,落盘后 fsync):

```json
{"type":"batch","batchId":"b-…","source":"s0","firstOffset":1024,"lastOffset":2048,
 "recordCount":50,"bytes":30000,"fingerprint":"sha256…","records":[…]}
```

`batchId` 对同样的来源、区间与内容是确定的(sha256),重启后对同一批工作得到同一
身份,下游按身份去重,重发安全。崩溃撕裂的半行按未发生处理,其记录因断点未覆盖
会被重新采集——不会丢,也不会重。

恢复断点 = 已确认 + 待提交 + 已失败批次覆盖的最大 `lastOffset`(压缩成
`checkpoint` 行持久化)。恢复时来源流从头读,但 `end ≤ 断点` 的记录直接跳过,
只重做尚未确认的工作。

## 提交与查证

- 正常提交:`POST /sink`,成功即在发件箱记 `done` 并释放预算。
- 整批拒收(422/400):记 `failed` 终态,事件 `batchRejected`,链路继续;
  失败批次留在发件箱可检查,不自动重试。
- 冲突(409):下游已有同身份不同内容的批次,记 `failed`,事件 `batchConflict`。
- 网络错误/超时/5xx:是否已被接受**未知**。重发之前先查证
  `GET /sink/:batchId`:有且指纹一致 → 直接记 `done`(事件 `batchVerified`,
  不重发);没有 → 重发;查不出 → 退避后重试。恢复路径对所有待提交批次同样先查证。
- 重试超过 `commitRetries` 仍联系不上下游:事件 `sinkUnreachable`,停止继续领取,
  未确认批次全部留在发件箱,运行以 `incomplete` 结束——下游恢复后用同一发件箱
  再跑即可继续处理。

## 取消语义

- `close()` 优雅关闭:立即中止各来源的预算领取与 socket 读取;来源把未成行的
  部分批次写入发件箱;提交器在 `drainMs` 内收拢仍可核对的提交;预算全部归还。
- `abort()` 硬中止:模拟进程被杀,不等待收拢。状态以发件箱为准,下次运行恢复。

## 运行观测

`pipeline.stats()` 与 `npm run collect` 的周期 JSON 行包含:各预算维度
`used/limit/waiting/waits/peak`、每来源状态与 `waitingOn`(正卡在哪个维度、
卡了多久)、提交队列与在途数、发件箱 `pending/done/failed`。最终报告附各维度
峰值与收紧次数。

## 验证(test/pipeline.test.mjs)

用本地限速来源与慢速接收器证明上限在关键场景都有效,数据量始终远大于预算:

- 并发 + 压缩膨胀:两来源各 ~800KB 解压数据,`rawBytes`/`inflatedBytes`/
  `pendingRecords` 峰值不超过上限且 `waits > 0`,结束后预算归零无泄漏。
- 慢速接收器:待提交记录预算收紧,采样能观察到来源卡在 `pendingRecords`。
- 公平性:慢流来源完成事件晚于快来源,但快来源不被拖住。
- 超大记录:skip 报告真实字节数且整流继续;fail 只影响该来源。
- 重启:dropAck(接受后丢确认)+ 硬中止 → 恢复时先查证,已接受批次零重发,
  全部记录恰好落库一次。
- 优雅关闭:预算归零,续跑后总量恰好一致。
- 拒收与失联:失败/待提交状态留在发件箱,可继续处理。
