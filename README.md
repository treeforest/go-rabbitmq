# go-rabbitmq

本库 [`github.com/treeforest/go-rabbitmq`](https://github.com/treeforest/go-rabbitmq)（包名 `mq`）基于 RabbitMQ AMQP 0-9-1 提供生产级异步消息发布、并发订阅、手动 Ack、队列级隔离延迟重试（Retry Queue）、死信队列（DLQ）以及断连自动自愈能力。包内直接使用 RabbitMQ 官方术语：`exchange`、`routing key`、`binding key`、`queue`、`ack`、`nack`。

发布时 `Publish` 的 `routingKey` 是这条消息的具体路由标签，不能包含 `*` 或 `#`。订阅时在 `SubscribeOptions.BindingKeys` 里配置队列的匹配规则。`direct` 模式下两者填同一个值；`topic` 模式下 `BindingKeys` 可以使用 `*` 与 `#`。

---

## 核心架构与内部机制原理

### 1. 队列级隔离拓扑与死信精准回流原理

一次 `Subscribe`（或 `EnsureTopology`）会自动幂等声明当前订阅链路所需的 3 个 Exchange 和 3 个 Queue。
假设配置为：
- `Exchange`: `"test.events"`（默认 `topic`）
- `Queue`: `"example-service.order-events"`
- `BindingKeys`: `[]string{"test.order.*"}`
- `RetryDelay`: `2 * time.Second`

数据流转与拓扑隔离关系如下：

```mermaid
flowchart LR
    P["Publisher<br/>Publish(routingKey='test.order.created')"] -->|"1. 业务发布"| EX["主 Exchange<br/>test.events (topic)"]
    EX -->|"BindingKey: test.order.*"| Q["主消费队列<br/>example-service.order-events"]
    Q -->|"2. 投递消费"| H["Consumer Handler<br/>(Concurrency Workers)"]
    H -->|"成功 (return nil)"| ACK["Ack(false)<br/>消费完成"]
    H -->|"失败 / Panic 且 DeliveryCount < MaxDeliveries"| REX["Retry Exchange<br/>test.events.retry (direct)"]
    REX -->|"RoutingKey = 主队列名"| RQ["专属 Retry Queue<br/>example-service.order-events.retry.2000<br/>(x-message-ttl=2000ms)"]
    RQ -->|"TTL 到期经默认交换机 ''<br/>x-dead-letter-routing-key = 主队列名"| Q
    H -->|"失败 / Panic 且 DeliveryCount >= MaxDeliveries"| DLX["Dead Letter Exchange<br/>test.events.dlx (direct)"]
    DLX -->|"RoutingKey = 主队列名"| DLQ["专属死信队列 DLQ<br/>example-service.order-events.dlq"]
```

**为什么这样设计？（防重试广播风暴）**
- 若同一个 `test.order.*` 下绑定了队列 A 和队列 B，队列 A 消费失败时：
  1. `mq` 包先将原始业务 `routingKey`（`test.order.created`）保存在内部头 `x-mq-original-routing-key` 中，并递增 `x-mq-retry-count`；
  2. 以队列名 `Queue` 作为 direct routing key 点对点发往 `.retry` 或 `.dlx` 交换机；
  3. Retry Queue 的 `x-dead-letter-exchange` 固定为 RabbitMQ 默认交换机 `""`，`x-dead-letter-routing-key` 固定为主队列名 `Queue`；
  4. TTL 到期后，消息**仅精准回流到队列 A**，绝不会再次经过主 Topic Exchange 扇出污染已成功消费过的队列 B；
  5. 消息回到队列 A 再次消费时，`toMessage` 自动从 `x-mq-original-routing-key` 还原 `msg.RoutingKey`，对业务层完全透明。

| 资源类型 | 名称示例 | 类型 / 属性 | 作用 |
| --- | --- | --- | --- |
| 主 Exchange | `test.events` | 默认 `topic`（支持 `direct`/`fanout`） | 接收业务发布消息并按 `BindingKeys` 路由到各主队列 |
| Retry Exchange | `test.events.retry` | `direct` | 按主队列名点对点路由失败待重试消息 |
| DLX | `test.events.dlx` | `direct` | 按主队列名点对点路由达到 `MaxDeliveries` 的死信消息 |
| 主消费队列 | `example-service.order-events` | `durable=true` | 业务消费者实际拉取消息的队列 |
| 延迟重试队列 | `example-service.order-events.retry.2000` | `durable=true`, `x-message-ttl=2000` | 存放延迟重试消息，TTL 到期后点对点死信回原主队列 |
| 死信队列 (DLQ) | `example-service.order-events.dlq` | `durable=true` | 存放最终失败消息，供告警、排查或离线重放 |

---

### 2. 无锁流水线发布确认（Confirm Pipelining）与 Return 精确配对原理

在 RabbitMQ AMQP 0-9-1 中，当开启 `Mandatory=true`（默认开启）且消息不可路由时，Broker 会在返回 `basic.ack` 之前先发回一条异步的 `basic.return` 帧。

为了同时实现**高吞吐并发 Confirm 流水线**与**零串扰的 Mandatory Return 检测**，`amqpPublisher` 采用如下内部机制：

```mermaid
sequenceDiagram
    participant C1 as 调用协程 1 (Mandatory=true)
    participant C2 as 调用协程 2 (Mandatory=true)
    participant P as amqpPublisher
    participant B as RabbitMQ Broker

    C1->>P: 1. registerReturnWaiter() -> 分配 seq=1, 注入头 x-mq-publish-seq=1
    C2->>P: 1. registerReturnWaiter() -> 分配 seq=2, 注入头 x-mq-publish-seq=2
    C1->>P: 2. sendMu.Lock() 写帧并立即 Unlock()
    P->>B: basic.publish (seq=1)
    C2->>P: 2. sendMu.Lock() 写帧并立即 Unlock()
    P->>B: basic.publish (seq=2)
    Note over C1,C2: 3. 两个协程在锁外并行等待各自的 confirm.WaitContext(ctx)
    B-->>P: basic.return (seq=1, NO_ROUTE)
    B-->>C1: basic.ack (delivery_tag=1)
    B-->>C2: basic.ack (delivery_tag=2)
    C1->>P: 4. takeReturn(seq=1) -> 精确命中 NO_ROUTE 错误
    C2->>P: 4. takeReturn(seq=2) -> 无 return，返回成功 nil
```

- **写帧串行、等待并行**：`sendMu` 仅包裹 `PublishWithDeferredConfirmWithContext` 写缓冲区瞬间，网络 RTT 等待阶段完全无锁；
- **超时隔离**：若某次调用因 `ctx` 超时提前退出，`defer unregisterReturnWaiter(seq)` 会立即注销其等待槽位；即便该请求的 `basic.return` 随后才迟到，也会在排空时因无匹配槽位而被安全丢弃，绝不会误伤后续正常请求。

---

### 3. 单飞断连自愈（Singleflight Reconnect）与防死锁停机机制

- **连接心跳与自动恢复**：
  1. 默认启用 `10s` AMQP 心跳（`DefaultHeartbeat`），快速感知 NAT 超时或半开死连接；
  2. `watchTransport` 监听 `Connection` 与发布 `Channel` 的 `NotifyClose` 事件，通过递增代数（`generation`）精准标记当前传输层失效；
  3. 后续 `Publish`、`EnsureTopology` 或后台订阅监督协程触发 `ensureConnected` 时，通过 **Singleflight（单飞）** 选举唯一领头协程执行重连，其余并发协程共享等待通道；
  4. 重连采用 **100ms 起始、最大 30s、附带 0~20% 加密随机抖动** 的指数退避；若原 `Connection` 仍存活则仅重建发布 `Channel`，否则重新拨号建连；
  5. 已注册的 `Subscription` 在连接恢复后自动重新打开消费 `Channel`、声明拓扑、设置 `Qos` 并继续消费，对外句柄保持不变。
- **半开连接防死锁停机**：
  - `Drain(ctx)` 仅发送 `basic.cancel` 停止接收新消息并等待在途 Handler 自然完成，**不取消**在途 Handler 的 `ctx`；
  - `Close()` 立即取消运行期 `ctx` 并在 `CloseTimeout` 内等待退出；
  - 若遇到半开连接导致 `basic.cancel` 网络 RPC 卡死，`Drain(ctx)` 或 `Close()` 超时后会通过独立的 `stateMu` + `TryLock` 强制关闭底层 `Channel` 唤醒阻塞协程，确保永不死锁。

---

## 快速上手与生产级完整示例

### 1. 启动本地 RabbitMQ

```bash
docker compose up -d rabbitmq
```

- AMQP 地址：`amqp://test:test123@127.0.0.1:5672/`
- 管理后台：`http://127.0.0.1:15672/`

### 2. 生产级端到端使用范式

```go
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	mq "github.com/treeforest/go-rabbitmq"
)

func run(ctx context.Context) error {
	// 1. 创建客户端（内置连接池化复用、10s 心跳与断连自动恢复）。
	client, err := mq.NewClient(mq.Config{
		URL:            "amqp://test:test123@127.0.0.1:5672/",
		Name:           "order-service",
		Exchange:       "test.events",
		ExchangeType:   "topic",
		ConnectTimeout: 5 * time.Second,
		PublishTimeout: 5 * time.Second,
		CloseTimeout:   5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	subOpts := mq.SubscribeOptions{
		Queue:         "example-service.order-events",
		BindingKeys:   []string{"test.order.*"},
		Prefetch:      32, // 若小于 Concurrency 会自动提升至 Concurrency
		Concurrency:   8,  // 8 个并发 Worker 协程池消费
		MaxDeliveries: 3,  // 最多投递 3 次（含首次），仍失败则进入 example-service.order-events.dlq
		RetryDelay:    2 * time.Second,
	}

	// 2. （可选）在发布前预声明拓扑，确保即使消费者尚未启动，消息也能持久落入主队列。
	if err := client.EnsureTopology(ctx, subOpts); err != nil {
		return err
	}

	// 3. 启动订阅（startupCtx 仅控制首次同步建连阶段）。
	// 取消 startupCtx 不会停止已建立的后台消费；停止需调用 sub.Drain、sub.Close 或 client.Close。
	startupCtx, cancelStartup := context.WithTimeout(ctx, 5*time.Second)
	defer cancelStartup()

	sub, err := client.Subscribe(startupCtx, subOpts, func(msgCtx context.Context, msg mq.Message) error {
		// 使用 msg.ID 做业务幂等；msg.DeliveryCount 为 1-based 投递次数（首次为 1）。
		// 内部控制头（x-mq-*）已自动过滤，不会污染 msg.Headers。
		fmt.Printf("recv id=%s key=%s delivery=%d body=%s\n", msg.ID, msg.RoutingKey, msg.DeliveryCount, string(msg.Data))

		// 返回 nil -> 自动 Ack；
		// 返回 error 或发生 panic -> 自动捕获并按 RetryDelay 转发至 Retry 队列；DeliveryCount >= MaxDeliveries 时进入 DLQ。
		return nil
	})
	if err != nil {
		return err
	}

	// 4. 发布消息（默认 Mandatory=true 且等待 Broker Confirm）。
	err = client.Publish(
		ctx,
		"test.order.created",
		[]byte(`{"order_id":"order-1"}`),
		mq.WithMessageID("order-created-1"),
		mq.WithHeader("event_type", "order.created"),
	)
	if err != nil {
		if strings.Contains(err.Error(), "delivery outcome unknown") {
			// 消息已提交至网络但确认超时/断连，若业务侧发起重试，请务必复用同一个 MessageID！
		}
		return err
	}

	// 5. 优雅停机：先 Drain 停止接收新消息并等待在途 Handler 完成，再 Close 释放通道。
	// 只调 client.Close() 会立刻取消在途 handler ctx；真优雅停机需先调 sub.Drain(drainCtx)。
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDrain()
	_ = sub.Drain(drainCtx)
	return sub.Close()
}
```

---

## Handler 返回值、Panic 与 Ack/Nack 决策表

| Handler 行为 | 包内自动动作 | 最终结果 |
| --- | --- | --- |
| 返回 `nil` | `Ack(false)` | 消息处理成功，从主队列移除 |
| 返回 `error` 或发生 `panic`（且 `DeliveryCount < MaxDeliveries`） | 点对点发布到 `.retry` 交换机，Confirm 成功后 `Ack` 原消息 | 在专属 Retry Queue 等待 `RetryDelay` 后精准死信回原主队列 |
| 返回 `error` 或发生 `panic`（且 `DeliveryCount >= MaxDeliveries`） | 点对点发布到 `.dlx` 交换机，Confirm 成功后 `Ack` 原消息 | 进入专属 `.dlq` 死信队列隔离，不再自动重试，绝不阻塞或搞崩进程 |
| 返回 `error`，且当前订阅 `ctx.Err() != nil`（正在关闭） | `Nack(false, true)` | 原消息直接重新入队，不消耗重试次数 |
| 返回 `error`，且转发 `.retry` / `.dlx` 失败 | `Nack(false, true)` | 原消息重新入队，确保极端故障下绝不丢消息 |

---

## 配置参数与默认值一览

| 参数 | 所属结构体 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `ExchangeType` | `Config` | `"topic"` | 支持 `"topic"`、`"direct"`、`"fanout"` |
| `ConnectTimeout` | `Config` | `5s` | 建立底层 TCP/AMQP 连接超时时间（内置 `10s` AMQP 心跳） |
| `PublishTimeout` | `Config` | `5s` | 单次 `Publish` 等待 Broker Confirm 的超时时间 |
| `CloseTimeout` | `Config` | `5s` | `Close()` 等待消费循环退出的最大时间 |
| `Mandatory` | `PublishOptions` | `true` | 为 `true` 时若消息无法路由到任何队列，Broker 会退回并报错 |
| `Prefetch` | `SubscribeOptions` | `32` | 单个消费通道的最大未确认消息预取数（若小于 `Concurrency` 会自动提升至 `Concurrency`） |
| `Concurrency` | `SubscribeOptions` | `1` | 单个订阅并发执行 Handler 的协程池大小 |
| `MaxDeliveries` | `SubscribeOptions` | `5` | 最大投递次数（1-based，含首次投递），达到后进入 DLQ |
| `RetryDelay` | `SubscribeOptions` | `1s` | 失败消息在专属 Retry Queue 中的等待时长（毫秒级精度，最小 1ms） |

---

## 语义边界与生产建议

- **至少一次（At-Least-Once）模型**：本库不承诺 Exactly-Once。连接闪断重连、或失败转发成功但随后的原消息 `Ack` 恰好断连失败时，Broker 可能会重复投递同一条消息。
- **幂等黄金法则**：发布端应始终通过 `mq.WithMessageID(...)` 传入稳定的业务唯一 ID，消费端 Handler 内按 `msg.ID` 做幂等去重。
- **区分未知投递结果**：若 `Publish` 返回的错误包含 `delivery outcome unknown`，代表消息已写入套接字但未收到 Confirm（可能已落库也可能未落库），业务重试时切勿更换 `MessageID`。
- **可观测性字段**：`msg.DeliveryCount` 是 1-based 投递次数（首次为 `1`，第 1 次重试为 `2`）。处理失败且 `DeliveryCount >= MaxDeliveries` 时进入 DLQ。内部控制头（`x-mq-retry-count`、`x-mq-original-routing-key`、`x-mq-publish-seq`）已自动剥离，不会污染 `msg.Headers`。

---

## 测试与代码质量审计

运行单元测试与竞态检测：

```bash
go test -race ./... -count=1
```

运行需要外部 RabbitMQ 的集成测试：

```bash
docker compose up -d rabbitmq
RABBITMQ_URL=amqp://test:test123@127.0.0.1:5672/ go test -tags=integration . -v
```

运行模拟 Broker 宕机重启（`docker compose down` → `up`）的端到端自愈测试（需单独运行）：

```bash
docker compose up -d rabbitmq
RABBITMQ_URL=amqp://test:test123@127.0.0.1:5672/ \
RABBITMQ_COMPOSE_RESTART=1 \
  go test -tags=integration . -run TestIntegrationRecoversAfterBrokerRestart -v -count=1
```
