# LlyKafka

一个简易的 Kafka 桌面管理工具，灵感来自 [Kafka-King](https://github.com/Bronya0/Kafka-King)。

- **桌面 UI**：[`fyne-io/fyne`](https://github.com/fyne-io/fyne)
- **底层框架**：[`gogf/gf`](https://github.com/gogf/gf)（配置、日志、JSON、文件、命令行参数、字符串/时间工具）
- **Kafka 客户端**：[`IBM/sarama`](https://github.com/IBM/sarama)

## 功能

| 模块 | 说明 |
| ---- | ---- |
| 连接管理 | 新增 / 编辑 / 删除多个 Kafka 集群连接（broker 地址、Kafka 版本、ClientID、备注） |
| 集群概览 | 显示集群全部 Broker，含控制器标识与 Kafka 版本 |
| Topic 管理 | 列表（带搜索）、新建、删除、按分区查看 Leader / Replicas / ISR / StartOffset / EndOffset / 消息数与 Topic 配置 |
| 消息浏览 | 按 Topic + 分区拉取：支持「最新 N 条」或「指定 Offset 范围」，显示 Offset / Key / Value / 时间 / 大小，可双击查看完整消息与 Headers |
| 消息发送 | 向任意 Topic / 分区发送一条消息（可选 Key、Headers） |
| 消费组 | 列出全部消费组及状态、成员数；查看每个分区的「消费 Offset / 最新 Offset / Lag」，Lag > 0 高亮告警 |

## 目录结构

```
lly_kafka/
├── main.go                       # 入口
├── config.yaml                   # gf gcfg 读取的应用配置
├── internal/
│   ├── core/                     # 业务核心层（GoFrame + sarama）
│   │   ├── core.go               # gf 初始化（gcfg + glog）
│   │   ├── config.go             # AppConfig 读取
│   │   ├── store.go              # 连接配置 JSON 持久化（gjson + gfile）
│   │   ├── kafka.go              # sarama 客户端封装
│   │   ├── broker.go             # Broker 列表
│   │   ├── topic.go              # Topic 增删查
│   │   ├── message.go            # 消息拉取与发送
│   │   └── group.go              # 消费组与 Lag
│   └── ui/                       # fyne 桌面 UI
│       ├── helpers.go            # 中文字体主题 + 通用对话框
│       ├── app.go                # 主窗口与连接管理
│       ├── connections.go        # 连接配置对话框
│       ├── brokers.go            # 集群 Tab
│       ├── topics.go             # Topic Tab
│       ├── messages.go           # 消息 Tab
│       └── groups.go             # 消费组 Tab
├── data/connections.json         # 自动生成，保存连接配置
├── logs/                         # 自动生成，glog 日志目录
└── README.md
```

## GoFrame 在哪些地方发挥作用

| 组件 | 用途 |
| ---- | ---- |
| `gcfg` | 加载 `config.yaml` |
| `glog` | 应用日志（自动读取 `logger` 段） |
| `gjson` | `data/connections.json` 解析与编码 |
| `gfile` | 文件存在检查、字节读取、目录创建 |
| `gconv` | 配置值到目标类型的自动转换 |
| `gstr` | 大小写不敏感的 Topic 搜索 |
| `gtime` | 消息时间戳格式化 |
| `gcmd` | `-config` 命令行参数解析 |

## 构建与运行

### 前置条件

- Go 1.22+（本仓库测试于 Go 1.25）
- Windows / macOS / Linux 桌面环境
- **CGO 编译器**：Fyne 桌面驱动依赖
  - Windows：安装 [MSYS2](https://www.msys2.org/) 并将 `mingw64/bin` 加入 `PATH`，或安装 MinGW-w64
  - macOS：Xcode Command Line Tools (`xcode-select --install`)
  - Linux：`gcc` + `pkg-config` + X11/Wayland 开发库（`libgl1-mesa-dev`、`xorg-dev` 等）

### 运行

```bash
go mod tidy
go run ./
```

首次启动时日志会输出到 `logs/`，连接配置保存在 `data/connections.json`。

### 自定义配置路径

```bash
go run ./ -config myconfig.yaml
```

### 打包为独立可执行

```bash
fyne package -os windows -icon ./icon.png  # 需要 icon，可省略
# 或直接
go build -o lly_kafka.exe .
```

> `fyne package` 会自动嵌入元数据并去掉命令行窗口。

## 使用示例

1. 启动后点击工具栏「**新建连接**」，填写：
   - 名称：例如 `dev-local`
   - Broker 地址：`127.0.0.1:9092`
   - Kafka 版本：选择与集群一致的版本（不识别时回退到 `2.6.0`）
   - Client ID：`lly-kafka`
2. 选择下拉框中的连接名，点击「**连接**」。状态栏显示「已连接: dev-local | Kafka 3.6.0 | 3 brokers」。
3. 切换到对应 Tab：
   - **集群**：查看所有 Broker，控制器标记 ✔。
   - **Topic**：左侧列表（支持搜索），右侧查看分区与配置；可「新建 Topic」/「删除 Topic」。
   - **消息**：选择 Topic 与分区，选择「最新 50 条」或「指定 Offset 范围」，点击「拉取消息」；双击某行可看完整消息内容。
   - **消费组**：左侧选择消费组，右侧查看每个分区的 Lag（>0 会标红并加 ⚠）。

## 中文显示

Fyne 默认字体不带中文字形，程序启动时会自动在常见系统路径下探测 CJK 字体（`simhei.ttf`、`msyh.ttc`、`PingFang.ttc`、`NotoSansCJK`、`wqy-microhei` 等）并加载为 UI 字体。

如果检测失败，可通过环境变量指定：

```bash
# Windows
set FYNE_FONT=C:\Windows\Fonts\simhei.ttf
go run ./

# Linux / macOS
FYNE_FONT=/path/to/your-cjk.ttf go run ./
```

## 简化之处（与 Kafka-King 相比）

- 仅支持 PLAINTEXT（无 SASL / SSL）
- 不支持消息内容 JSON / Avro 自动格式化（仅预览字节）
- 消息查看为单线程同步拉取，超时 5 秒
- 不包含消费者注册、KSQL、ACL 等高级特性

## 许可

MIT