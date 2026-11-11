// Package mq 基于 RabbitMQ AMQP 0-9-1 提供生产级服务间异步消息发布与订阅能力。
//
// 核心特性与设计机制：
//  1. 官方术语直通：直接使用 exchange、routing key、binding key、queue、ack、nack，无二次概念包装。
//  2. 无锁流水线发布确认（Confirm Pipelining）：底层仅在写帧瞬间持有互斥锁，所有消息（含默认 Mandatory=true）
//     均在锁外并发等待 Broker Publisher Confirm；通过内部序号头 x-mq-publish-seq 精确配对各自的 basic.return。
//  3. 队列级隔离重试与死信（Retry & DLQ）：每次订阅自动声明主队列、专属延迟重试队列（<queue>.retry.<ms>）和专属死信队列（<queue>.dlq）。
//     Message.DeliveryCount 为 1-based 投递次数（首次为 1）。处理失败且 DeliveryCount 未达 SubscribeOptions.MaxDeliveries 时，
//     经 .retry（direct）点对点进入重试队列，TTL 到期后由默认交换机 "" 按队列名精准死信回原主队列；达到 MaxDeliveries 后进入 DLQ。
//  4. 单飞断连自愈（Singleflight Reconnect）：连接或发布通道被动关闭后，自动通过单飞模式与加密随机抖动指数退避重建传输层，
//     已创建的 Subscription 在后台透明重新声明拓扑、设置 Qos 并恢复消费。
//  5. 生产防弹保障：内置业务 Handler Panic 捕获（自动转入 Retry/DLQ 防止毒消息拖垮进程）、AMQP 心跳检测（默认 10s）、
//     Prefetch >= Concurrency 自动对齐，以及半开连接下 Drain/Close 强制打断阻塞 RPC 的防死锁机制。
//
// 投递语义边界：
//   - 本包提供至少一次（At-Least-Once）投递保证，不承诺 Exactly-Once。
//   - 当 Publish 返回包含 "delivery outcome unknown" 的错误时，表示消息已写出但确认结果未知（如超时或断连），
//     包内不会自动重发；调用方应传入稳定的 MessageID 并在消费端基于 Message.ID 实现业务幂等。
package mq
