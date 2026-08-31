# LlyKafka

一个 Kafka 桌面管理工具，灵感来自 [Kafka-King](https://github.com/Bronya0/Kafka-King)。

- **桌面 UI**：[`fyne-io/fyne`](https://github.com/fyne-io/fyne)
- **底层框架**：[`gogf/gf`](https://github.com/gogf/gf)
- **Kafka 客户端**：[`IBM/sarama`](https://github.com/IBM/sarama)

## 功能

| 模块 | 说明 |
| ---- | ---- |
| 连接管理 | 多集群；SASL PLAIN / SCRAM-SHA-256/512；TLS（CA / 客户端证书 / 跳过校验）；测试连通；编辑当前连接后自动重连 |
| 集群概览 | Broker 列表、控制器、协议版本 |
| Topic | 搜索、新建、删除（含分区数确认）、扩分区、改配置；ISR 不足高亮；分区 offset / 消息数 |
| 消息 | **筛选 Topic**；选中 Topic/分区后按当前模式**自动拉取**；最新 N 条 / Offset 范围 / **按起始时间**；单分区或**全部**；拉取可取消；Key/Value 过滤；JSON 缩进 / 二进制 hex；指定分区发送；**导出 JSON**；全分区拉取部分失败时显示失败分区明细 |
| 消费组 | 搜索；分区 Lag 与总 Lag（未提交显示 `-`）；**成员详情（ClientID / Host / 分区分配）**；重置到最新 / 最早 |
| 审计日志 | 所有写操作（创建/删除 Topic、扩分区、改配置、发送消息、重置 Offset）自动记录到 `logs/audit-{Ymd}.log`，含操作类型、目标、连接名、成功/失败及错误信息 |

窗口大小、上次连接、消息 Tab 的 Topic/分区选择会自动记住。切到消息 Tab 会刷新 Topic 列表。异步刷新会忽略过期请求；断开连接、取消拉取或关闭窗口不会再将旧结果写回界面。连接过程中会锁定连接下拉，避免选中项与实际连接不一致。

## 目录结构

```
lly_kafka/
├── main.go
├── config.yaml
├── FyneApp.toml
├── internal/core/          # Kafka + 配置 + 存储 + 审计
│   ├── kafka.go            # KafkaClient 封装（连接/关闭/懒加载）
│   ├── topic.go            # Topic CRUD、分区详情、配置管理
│   ├── message.go          # 消息拉取（最新/范围/时间/全分区）、发送、分区错误聚合
│   ├── group.go            # 消费组列表、Offset/Lag、成员详情、重置
│   ├── security.go         # 4种安全协议 + SASL/TLS 配置
│   ├── audit.go            # 写操作审计日志
│   ├── store.go            # 连接配置 JSON 持久化（原子写入+权限控制）
│   └── ...
├── internal/ui/            # fyne 界面
│   ├── app.go              # 主窗口、工具栏、Tab 容器
│   ├── messages.go         # 消息浏览（筛选 Topic、自动拉取、过滤/详情/发送/导出）
│   ├── filter_select.go    # 带筛选的下拉选择
│   ├── groups.go           # 消费组（列表/Offset-Lag/成员详情/重置）
│   └── ...
├── data/connections.json   # 自动生成（相对配置文件目录）
└── logs/
    ├── {Y-m-d}.log         # 运行日志
    └── audit-{Ymd}.log     # 审计日志
```

配置、连接文件、日志默认相对 **config.yaml 所在目录**。开发时用仓库根目录的 `config.yaml`；把 exe 和 `config.yaml` 放在同一目录即可独立运行。TLS 的 CA、客户端证书和私钥路径也遵循相同规则。

```bash
go run ./
go run ./ -config D:\cfg\config.yaml
```

## 连接安全

| 安全协议 | 说明 |
| -------- | ---- |
| PLAINTEXT | 无认证 |
| SASL_PLAINTEXT | SASL，无 TLS |
| SSL | 仅 TLS |
| SASL_SSL | SASL + TLS |

SASL 支持 `PLAIN`、`SCRAM-SHA-256` 和 `SCRAM-SHA-512`。TLS 默认启用证书校验，并使用首个 Broker 的主机名校验证书；仅在测试环境使用“跳过证书校验”。

SASL 密码保存在本地 `data/connections.json`，请勿把该文件提交到仓库。程序使用原子写入；在支持 POSIX 权限的系统上，连接目录和文件仅允许当前用户访问。共享设备仍建议使用独立 OS 账户或系统凭据服务。

## 操作说明与注意事项

- **消息拉取**：
  - Topic 支持关键字筛选（不区分大小写）；回车可选中唯一匹配或精确名称。发送消息对话框同样可筛选。
  - 选中 Topic 或切换分区后，按当前模式自动拉取。分区加载完成前「拉取消息」会禁用。
  - 支持三种模式：「最新 N 条」、「指定 Offset 范围」、「按起始时间」。结束 Offset 为 `0` 表示拉取到最新消息。
  - 按时间模式支持多种输入格式：`2026-08-26 12:00:05`、`2026-08-26 12:00`、`2026-08-26`，也支持 `/` 分隔。未填时间时不会自动拉取。
  - 取消或超时会明确显示已收到的消息数。全部分区拉取若有分区失败，会弹出信息框列出具体失败的分区编号和错误（如 `部分分区拉取失败 (2/8): 分区 3: ...; 分区 5: ...`）。
- **消息导出**：在消息 Tab 点击「导出 JSON」，可将当前过滤后的消息导出为 JSON 文件，包含 topic、partition、offset、key、value（自动格式化）、headers、timestamp、size 等完整字段。
- **消费组 Lag**：从未提交的分区 Offset / Lag 显示为 `-`，不计入总 Lag。
- **消费组成员**：选中消费组后，详情区域会展示成员列表，包括每个成员的 ClientID、Host 以及分配的 Topic/分区。正在 rebalance 的组可能暂时无成员分配。筛选导致当前组不可见时，成员详情会一并清空。
- **消费组 Offset 重置**：重置到最新/最早会影响该组的全部已提交分区。请先停止消费者，确认不再有正在处理的消息，再执行操作。
- **连接编辑**：保存正在使用的连接配置后会按新参数自动重连。名称、Broker 等必填项校验失败时对话框会重新打开，已填内容保留。
- **Topic 管理**：扩分区只能增加，不能减少；删除 Topic、修改配置和重置 Offset 都应在生产环境经过变更审批。新建 Topic / 扩分区 / 改配置校验失败时同样会保留对话框。
- **凭据安全**：连接配置包含密码时，请保证 `data/` 不被同步到公共网盘、备份仓库或共享目录。
- **审计日志**：所有写操作自动记录到 `logs/audit-{Ymd}.log`，便于追溯操作历史。日志仅写入文件，不输出到控制台。

## 构建

需要 Go 1.25+ 与 CGO（Fyne）。Windows 需 MinGW/MSYS2。

```bash
go test ./...
go test -race ./...
go vet ./...
go build -o lly_kafka.exe .
# 或
fyne package -os windows
```

## 中文显示

启动后异步加载系统 CJK 字体。也可设置 `FYNE_FONT` 指向 ttf/ttc。

## 许可

MIT
