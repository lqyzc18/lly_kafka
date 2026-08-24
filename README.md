# LlyKafka

一个 Kafka 桌面管理工具，灵感来自 [Kafka-King](https://github.com/Bronya0/Kafka-King)。

- **桌面 UI**：[`fyne-io/fyne`](https://github.com/fyne-io/fyne)
- **底层框架**：[`gogf/gf`](https://github.com/gogf/gf)
- **Kafka 客户端**：[`IBM/sarama`](https://github.com/IBM/sarama)

## 功能

| 模块 | 说明 |
| ---- | ---- |
| 连接管理 | 多集群；SASL PLAIN / SCRAM-SHA-256/512；TLS（CA / 客户端证书 / 跳过校验）；测试连通 |
| 集群概览 | Broker 列表、控制器、协议版本 |
| Topic | 搜索、新建、删除（含分区数确认）、扩分区、改配置；ISR 不足高亮；分区 offset / 消息数 |
| 消息 | 最新 N 条或 Offset 范围；单分区或**全部**；拉取可取消；Key/Value 过滤；JSON 缩进 / 二进制 hex；指定分区发送 |
| 消费组 | 搜索；分区 Lag 与总 Lag；重置到最新 / 最早 |

窗口大小、上次连接、消息 Tab 的 Topic/分区选择会自动记住。异步刷新会忽略过期请求；断开连接、取消拉取或关闭窗口不会再将旧结果写回界面。

## 目录结构

```
lly_kafka/
├── main.go
├── config.yaml
├── FyneApp.toml
├── internal/core/          # Kafka + 配置 + 存储
├── internal/ui/            # fyne 界面
├── data/connections.json   # 自动生成（相对配置文件目录）
└── logs/
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

- **消息拉取**：指定 Offset 范围时，结束 Offset 为 `0` 表示拉取到最新消息；取消或超时会明确显示已收到的消息数。全部分区拉取若有分区失败，会提示部分结果错误。
- **消费组 Offset 重置**：重置到最新/最早会影响该组的全部已提交分区。请先停止消费者，确认不再有正在处理的消息，再执行操作。
- **Topic 管理**：扩分区只能增加，不能减少；删除 Topic、修改配置和重置 Offset 都应在生产环境经过变更审批。
- **凭据安全**：连接配置包含密码时，请保证 `data/` 不被同步到公共网盘、备份仓库或共享目录。

## 构建

需要 Go 1.22+ 与 CGO（Fyne）。Windows 需 MinGW/MSYS2。

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
