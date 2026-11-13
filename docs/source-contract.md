# 来源协议

GET /stream 按count与recordBytes生成NDJSON并gzip，通过chunkBytes限制网络块，delay控制块间隔。POST /sink 接受batchId及records；同身份同内容返回原确认，不同内容409；rejectBatch使整批422，dropAck在实际接受后断开响应。GET /sink/:batchId查确认。控制值均使用本地毫秒，单条记录大小可大于网络块。源服务尊重socket.write的drain，不把客户端背压改成来源无限缓存。

## 通用控制

`GET /inventory` 返回本来源当前目录，`GET /health` 检查存活，`GET /state` 查看来源状态。`POST /control` 接收JSON对象，仅在来源支持的字段范围内操作。每次创建服务重新读取 fixtures/source.json；控制用于复现来源演进，不是采集业务的实现接口。

服务使用随机本地端口，测试启动与关闭均由代码负责。没有真实个人资料、凭据或付费调用。
