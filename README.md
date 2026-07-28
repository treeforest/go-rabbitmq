# go-rabbitmq

本库 [`github.com/treeforest/go-rabbitmq`](https://github.com/treeforest/go-rabbitmq)（包名 `mq`）基于 RabbitMQ AMQP 0-9-1 提供发布、订阅、手动 ack、失败重试和 DLQ 能力。包内直接使用 RabbitMQ 官方术语：exchange、routing key、binding key、queue、ack、nack。

## 快速上手

1. 启动 RabbitMQ：

```bash
docker compose up -d rabbitmq
```

默认连接地址：

```text
amqp://test:test123@127.0.0.1:5672/
```

管理后台地址：

```text
http://127.0.0.1:15672/
```

2. 创建客户端：

```go
import mq "github.com/treeforest/go-rabbitmq"

client, err := mq.NewClient(mq.Config{
	URL:      "amqp://test:test123@127.0.0.1:5672/",
	Name:     "example-app",
	Exchange: "test.events",
})
if err != nil {
	return err
}
defer client.Close()
```

3. 订阅消息：

```go
sub, err := client.Subscribe(ctx, mq.SubscribeOptions{
	Queue:       "example-service.order-events",
	RoutingKey:  "test.order.created",
	BindingKeys: []string{"test.order.*"},
	Prefetch:    16,
	MaxDeliver:  3,
	RetryDelay:  2 * time.Second,
}, func(ctx context.Context, msg mq.Message) error {
	// 这里做业务处理。返回 nil 表示成功，包内会 ack。
	// 返回 error 表示失败，包内会按 retry/DLQ 规则处理。
	return handleOrder(ctx, msg)
})
if err != nil {
	return err
}
defer sub.Close()
```

4. 发布消息：

```go
err = client.Publish(
	ctx,
	"test.order.created",
	[]byte(`{"order_id":"order-1"}`),
	mq.WithMessageID("order-created-1"),
	mq.WithHeader("event_type", "order.created"),
)
if err != nil {
	return err
}
```

发布端必须使用具体 routing key，不能包含 `*` 或 `#` 通配符。订阅端 `BindingKeys` 可以使用 RabbitMQ topic 通配符：`*` 匹配一个单词，`#` 匹配零个或多个单词。

集成测试命令：

```bash
docker compose up -d rabbitmq
RABBITMQ_URL=amqp://test:test123@127.0.0.1:5672/ go test -tags=integration . -v
```

模拟 RabbitMQ 宕机重启（`docker compose down` → `up`）后的重连测试默认跳过，需显式开启：

```bash
docker compose up -d rabbitmq
RABBITMQ_URL=amqp://test:test123@127.0.0.1:5672/ \
RABBITMQ_COMPOSE_RESTART=1 \
  go test -tags=integration . -run TestIntegrationRecoversAfterBrokerRestart -v -count=1
```

该测试会重启共享 broker，请单独运行，不要与其它集成测试并行。

## 拓扑说明

一次 `Subscribe` 会确保当前订阅链路需要的 exchange、queue 和 binding 都存在。假设：

```go
Exchange:   "test.events"
Queue:      "example-service.order-events"
BindingKey: "test.order.*"
RetryDelay: 2 * time.Second
```

会声明这些 exchange：

| 名称 | 类型 | 作用 |
| --- | --- | --- |
| `test.events` | 默认 `topic` | 主 exchange。业务发布消息到这里，正常消息从这里路由到主队列。 |
| `test.events.retry` | 默认 `topic` | 延迟重试 exchange。handler 失败但未达到最大投递次数时，消息先转发到这里。 |
| `test.events.dlx` | 默认 `topic` | Dead Letter Exchange。handler 失败且达到最大投递次数时，消息转发到这里。 |

会声明这些 queue：

| 名称 | 作用 |
| --- | --- |
| `example-service.order-events` | 主消费队列。消费者从这里接收业务消息。 |
| `example-service.order-events.retry.2000` | retry queue。`2000` 表示 `RetryDelay` 的毫秒数。消息进入这里后等待 TTL，到期后通过 dead-letter 回到主 exchange。 |
| `example-service.order-events.dlq` | DLQ，Dead Letter Queue。超过最大投递次数后进入这里，供人工排查、补偿或离线重放。 |

三类队列都会按 `BindingKeys` 绑定到对应 exchange：

| 队列 | 绑定到 | 说明 |
| --- | --- | --- |
| 主队列 | 主 exchange | 正常业务消息投递。 |
| retry queue | retry exchange | 失败消息先进入 retry queue，等待 TTL。 |
| DLQ | DLX | 最终失败消息进入 DLQ。 |

## 消息处理流程

正常成功路径：

1. 发布方调用 `Publish(ctx, routingKey, data, ...)`。
2. 消息发布到主 exchange，例如 `test.events`。
3. RabbitMQ 根据 routing key 和 binding key 把消息路由到主队列。
4. `Subscribe` 的 consumer 收到消息并调用 handler。
5. handler 返回 `nil`。
6. `mq` 包调用 `delivery.Ack(false)`，确认消息处理成功。

业务失败但可重试：

1. handler 返回 `error`。
2. 当前 context 没有取消。
3. `mq` 包读取消息头 `x-mq-retry-count`，计算下一次投递次数。
4. 如果下一次投递次数小于 `MaxDeliver`，消息会被转发到 retry exchange，例如 `test.events.retry`。
5. retry exchange 把消息路由到 retry queue，例如 `example-service.order-events.retry.2000`。
6. retry queue 的 `x-message-ttl` 到期后，RabbitMQ 通过 `x-dead-letter-exchange` 把消息重新投递回主 exchange。
7. 主 exchange 再次按原 routing key 路由回主队列。
8. retry 消息转发成功后，`mq` 包会 ack 原消息，避免原消息和 retry 消息同时存在。

业务失败且达到最大投递次数：

1. handler 返回 `error`。
2. 下一次投递次数大于等于 `MaxDeliver`。
3. 消息被转发到 DLX，例如 `test.events.dlx`。
4. DLX 把消息路由到 DLQ，例如 `example-service.order-events.dlq`。
5. DLQ 中的消息不会再自动消费，通常用于排查、告警、人工补偿或后续重放。
6. 转发到 DLQ 成功后，`mq` 包会 ack 原消息。

上下文取消或关闭中的失败：

1. 如果 handler 返回错误时 `ctx.Err() != nil`，说明订阅正在取消或超时。
2. `mq` 包会调用 `delivery.Nack(false, true)`。
3. `requeue=true` 表示让 RabbitMQ 重新入队，后续可由当前或其它 consumer 再消费。

失败转发异常：

1. 如果 handler 失败后，转发到 retry exchange 或 DLX 也失败。
2. `mq` 包会尝试 `delivery.Nack(false, true)`，让原消息重新入队。
3. 这样可以避免原消息在失败转发时丢失。

## Ack、Nack 和投递次数

handler 只需要通过返回值表达处理结果：

| handler 返回 | 包内行为 | 结果 |
| --- | --- | --- |
| `nil` | `Ack(false)` | 消息处理成功，从主队列移除。 |
| `error`，未达到 `MaxDeliver` | 发布到 retry exchange，成功后 ack 原消息 | 延迟后再次回到主队列。 |
| `error`，达到 `MaxDeliver` | 发布到 DLX，成功后 ack 原消息 | 进入 DLQ，不再自动重试。 |
| `error`，且 context 已取消 | `Nack(false, true)` | 原消息重新入队。 |
| `error`，且转发 retry/DLX 失败 | `Nack(false, true)` | 原消息重新入队，避免丢失。 |

投递次数保存在消息头：

```text
x-mq-retry-count
```

第一次失败后写入 `1`，下一次失败递增。达到 `MaxDeliver` 时进入 DLQ。默认值：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `Prefetch` | `32` | 单个 consumer 预取消息数量。 |
| `MaxDeliver` | `5` | 最大投递次数，达到后进入 DLQ。 |
| `RetryDelay` | `1s` | retry queue 的 TTL。 |
| `PublishTimeout` | `5s` | 等待 publisher confirm 的超时时间。 |
| `CloseTimeout` | `5s` | 关闭订阅和连接的等待时间。 |

## 关闭订阅

`Subscription` 提供两个关闭方法：

| 方法 | 适用场景 | 行为 |
| --- | --- | --- |
| `Drain(ctx)` | 优雅停机 | 发送 `basic.cancel`，不再接收新消息，并等待已投递消息处理完成。 |
| `Close()` | 普通释放资源 | 取消 consumer，等待消费循环退出，并释放 AMQP channel。 |

订阅关闭时，`mq` 包会串行化 `ack/nack/cancel/close`，避免同一个 AMQP channel 上的确认和关闭命令交错。

## 连接恢复

`Client` 会在 connection 或 publish channel 被动关闭后自动恢复，调用方无需重启进程或重新创建客户端：

1. 监听 connection / publish channel 的 `NotifyClose`，把当前传输层标记为不可用。
2. 后续 `Publish`、`EnsureTopology`、`Subscribe` 会触发单飞重连：同一时刻只有一个 Dial/重建流程。
3. 重连使用指数退避（初始约 `100ms`，最大约 `30s`），并受调用方 `context` 与 `Client.Close()` 约束。
4. 连接仍可用时优先重建 publish channel；否则重新 Dial，并重新启用 publisher confirm。
5. 已建立的 `Subscription` 会自动重新声明拓扑、重新设置 Qos 并重新 Consume；同一个 `Subscription` 句柄保持有效。

语义边界：

- 这是至少一次（at-least-once）模型，不承诺 exactly-once。
- 若消息已提交给 broker 但 confirm 尚未返回（超时、连接中断等），`Publish` 会返回带 `delivery outcome unknown` 的错误，**不会自动补发**，避免重复投递。
- 调用方应继续为消息设置稳定的 `MessageID`，并在 handler 内按 `msg.ID` 做幂等。
- 断连期间库内不会在内存中缓存待发布消息；发布失败由调用方决定是否重试。
- 连接恢复后，broker 可能重新投递未 ack 消息；重复消费应被视为正常情况。
- `Client.Close()` 后不会再重连，也不会恢复任何订阅。

## 建议

- `Queue` 建议带上服务名和业务名，例如 `example-service.order-events`，方便在 RabbitMQ 控制台定位。
- `MessageID` 建议使用业务唯一 ID，handler 内按 `msg.ID` 做幂等。
- handler 返回错误前不要自行 ack/nack，统一交给 `mq` 包管理。
- DLQ 要配套告警或补偿工具，否则最终失败消息只会堆积在队列里。
- `RetryDelay` 会参与 retry queue 名称，同一个业务队列如果使用多个 delay，会产生多个 retry queue。
- 业务侧重试 `Publish` 时，请区分可重试错误与 `delivery outcome unknown`；后者需要按幂等语义处理，不能盲目再发同一条业务事件。
