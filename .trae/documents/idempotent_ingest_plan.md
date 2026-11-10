# 告警接收 API 来源级批次幂等 实现计划

## 需求拆解

1. 调用方**可选**提供来源级批次幂等标识；不提供时完全沿用现有接收语义。
2. 相同 (来源, 批次) 重复提交：直接幂等成功，不再调用 `provider.Alerts.Put`，即不再驱动分组/通知。
3. 同一来源下，较旧批次（序号更小）不能覆盖较新批次状态 → 可识别错误，不写入。
4. 幂等记录落盘，进程重启后仍然有效（复用 `--storage.path` 数据目录，与 nflog/silences 同级）。
5. 批次标识不完整（只给了部分头）、批次状态不完整（崩溃残留 pending 记录）、持久化失败：返回**可识别错误**（导出的 sentinel error + 固定前缀消息 + 合适 HTTP 状态码）。

## 仓库研究结论

- 接收入口：[api.go postAlertsHandler](file:///Users/tog/my/code/swe/workspaces/alertmanager-idempotent-ingest-T04/api/v2/api.go#L348-L418)，校验后调用 `api.alerts.Put(ctx, validAlerts...)`；测试中大量以字面量构造 `API{}`，新增字段必须是 nil 即禁用的接口字段，避免破坏现有测试。
- 持久化范式：[nflog.go](file:///Users/tog/my/code/swe/workspaces/alertmanager-idempotent-ingest-T04/nflog/nflog.go) 采用「内存 state + 快照文件 + Maintenance 周期 GC/快照 + 停机最后一次快照」，`openReplace` 原子替换（fsync + rename）。silences 同构。新组件照此范式实现，但不引入新 proto（避免 protoc 依赖），使用 JSON Lines 事件日志。
- 装配点：[app.go setup](file:///Users/tog/my/code/swe/workspaces/alertmanager-idempotent-ingest-T04/app/app.go#L281-L368)，数据目录 `opts.DataDir`、保留期 `opts.Retention`、维护间隔 `opts.MaintenanceInterval`、统一 `stopc/wg` 维护协程与 onStop 拆卸。
- API 包装层 [api/api.go](file:///Users/tog/my/code/swe/workspaces/alertmanager-idempotent-ingest-T04/api/api.go#L185-L202) 是 `apiv2.NewAPI` 唯一生产调用点；`api.Options` 透传依赖。
- OpenAPI：postAlerts 当前只有 body 参数（[openapi.yaml L205-223](file:///Users/tog/my/code/swe/workspaces/alertmanager-idempotent-ingest-T04/api/v2/openapi.yaml#L205-L223)）。go-swagger 生成的 `PostAlertsParams` 始终携带 `HTTPRequest`，处理器可直接读自定义请求头，**运行时不依赖重新生成代码**；本机有 Docker，将尝试 `make apiv2`，若镜像拉取受限则只更新 openapi.yaml 契约文档、不改生成代码。
- Connect API 只有 status，无告警接收路径，无需改动。amtool 不发幂等头，自动走旧路径。

## 接口设计（HTTP 契约）

POST `/api/v2/alerts` 新增三个**可选**请求头，全有或全无：

| Header | 含义 |
|---|---|
| `X-Alertmanager-Source-Id` | 发送来源标识（字符串） |
| `X-Alertmanager-Batch-Id` | 该来源内唯一的批次标识（字符串） |
| `X-Alertmanager-Batch-Sequence` | 该来源内单调递增的批次序号（uint64） |

- 三个头都缺省 → 旧语义，零行为变化。
- 只出现部分 / 序号无法解析 → 400，`idempotency: incomplete batch identifier...`。
- 同 (source, batchID) 已完成 → 直接 200，不执行 Put（重试安全）。
- 同 (source, batchID) 处于 pending（上次处理中途崩溃）→ 400 `idempotency: incomplete batch state...`，拒绝重放，要求客户端换新批次号。
- 新批次但 `sequence <= 该来源已见最大序号` → 400 `idempotency: stale batch...`，旧批次不能覆盖新状态。
- 落盘失败 → 500 `idempotency: failed to persist batch state...`，内存改动回滚，不触发 Put。

## 新增/修改文件

### 新增 `idempotency/` 顶层包
- `idempotency/idempotency.go`
  - `Recorder`：互斥锁保护 `map[source]*sourceState{LastSequence, Batches map[batchID]*BatchRecord}`。
  - `BatchRecord`：Source/BatchID/Sequence/State(pending|completed)/AlertCount/CreatedAt/UpdatedAt/ExpiresAt。
  - 持久化：`<dataDir>/idempotency` 单文件 JSON Lines 事件流（`begin`/`complete` 事件）。状态转换时 O_APPEND 追加并 `fsync`（保证重启有效）；`New` 全量回放重建；尾行半写入（torn write）容忍并截断，非尾部损坏返回 `ErrInvalidState`。
  - `Maintenance(interval, stopc)`：周期 GC 过期记录（按 Retention）并压缩重写（临时文件 + fsync + rename，复用 nflog 的 openReplace 模式），停机时最后执行一次。
  - API：`Begin(ctx, source, batchID, seq) (Outcome, error)`、`Complete(ctx, source, batchID, alertCount) error`、`Close()`。
  - 导出 sentinel：`ErrStaleBatch`、`ErrIncompleteBatchState`、`ErrIncompleteIdentifier`、`ErrInvalidState`，全部包装为 `fmt.Errorf("...: %w", err)`，支持 `errors.Is`。
  - 指标：`alertmanager_batch_idempotency_events_total{outcome=accepted|duplicate|stale|incomplete|persist_failed}`、`alertmanager_batch_idempotency_records` gauge（registerer 可 nil）。
  - `SnapshotFile == ""` 时为纯内存模式（供嵌入方/单测），写操作 no-op。
- `idempotency/idempotency_test.go`：接受/重复/pending/陈旧序号/重启回放/GC 压缩/torn 尾行/损坏文件/不可写路径错误。

### 修改 `api/v2/`
- 新增 `api/v2/idempotency.go`：头常量、`BatchRecorder` 接口（消费侧定义）、`parseBatchHeaders` 解析与全有全无校验。
- `api/v2/api.go`：`API` 增加 nil 接口字段 + `SetBatchRecorder`；`postAlertsHandler` 在校验/Put 前插入 Begin，重复批次短路 200；Put 成功后、validationErrs 响应前 Complete（保证部分告警非法时批次仍闭环，重试不重复入库合法部分）；错误按上表映射 400/500。
- `api/v2/api_test.go`：新增处理器级测试（putRecorder 计数 Put 次数 + 真实 Recorder 临时目录 + 故障 fake）。

### 修改装配与契约
- `api/api.go`：`Options` 增加 `BatchRecorder` 并透传给 v2。
- `app/app.go`：构造 `idempotency.Recorder`（快照文件 `filepath.Join(opts.DataDir, "idempotency")`），加入现有 wg/stopc 维护协程与 onStop；传入 `api.Options`。
- `api/v2/openapi.yaml`：postAlerts 增加三个 optional header 参数及说明；尝试 `make apiv2` 重新生成（失败则保留手写处理器读头方案，生成代码不动）。
- `docs/alerts_api.md`：补充幂等头用法、语义与错误说明（更新现有文件）。

## 实现步骤（依赖顺序）

1. 新建 `idempotency` 包：类型、sentinel、WAL 回放/追加/fsync、GC 压缩、Maintenance、指标，配单测并跑通。
2. `api/v2`：头解析 + 接口 + handler 接入 + 单测（含故障注入、重启场景）。
3. `api/api.go` 与 `app/app.go` 装配；`go build ./...`。
4. 更新 openapi.yaml，尝试 `make apiv2`；更新 docs/alerts_api.md。
5. 全量验证：`go test ./idempotency ./api/... ./app/...`、`go test ./...`（视耗时）、`gofmt`/可用的 lint。

## 依赖与注意事项

- 仅用标准库 + 已有 prometheus client 依赖，不引新第三方库、不新增 proto。
- 顺序保证依赖客户端提供的单调 sequence；同批次重试靠 (source, batchID) 去重，两者覆盖需求中两个独立条款。
- 每条带幂等头的请求在状态转换时同步 fsync，吞吐以批请求为单位，可接受；压缩控制文件体积。
- HA 场景下幂等状态为节点本地、不做 gossip（需求只要求进程重启有效，未要求跨节点）；文档注明重试应尽量落同一节点。属明确范围外事项。

## 验证

- 单测断言：重复批次 Put 仅一次；旧序号被拒；无头时行为不变；重启新 Recorder 后重复仍被拒；pending 残留返回可识别错误；不可写存储返回可识别 500。
- `go build ./...`、目标包 `go test`、gofmt 格式化；若 swagger 镜像可用则完成 API 代码再生成并跑 `go test ./api/...`。

## 风险

- Docker 镜像拉取受限导致无法再生成 swagger：处理器直接读 `HTTPRequest.Header`，运行时不依赖生成代码，openapi.yaml 保持契约更新，后续 CI 可再生成。
- 同步 fsync 增加写延迟：以批为单位且数据目录本就是本地磁盘，配合周期压缩，风险可控；纯内存部署（SnapshotFile 为空）不受影响。
- 崩溃发生在 Begin 之后、Complete 之前：记录残留 pending，重试收到明确的 incomplete 错误而非静默重复驱动，符合需求第 5 点。
