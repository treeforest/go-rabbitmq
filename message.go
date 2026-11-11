package mq

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	retryCountHeader         = "x-mq-retry-count"
	originalRoutingKeyHeader = "x-mq-original-routing-key"
	publishSeqHeader         = "x-mq-publish-seq"
)

// Message 表示从 RabbitMQ 收到的一条消息。
type Message struct {
	// Exchange 表示消息来源 exchange。
	Exchange string

	// Queue 表示当前消费消息的队列名称。
	Queue string

	// RoutingKey 表示消息发布时使用的 routing key。
	RoutingKey string

	// Data 表示消息体原始字节。
	Data []byte

	// Headers 表示业务消息头（不包含 x-mq-* 内部控制头）。
	Headers map[string]string

	// ID 表示消息唯一标识，用于业务幂等或追踪。
	// 若上游发布时未设置 AMQP message_id，则回退为 "<routingKey>:<deliveryTag>"，仅可用于单次追踪，不具备跨重试幂等性。
	ID string

	// DeliveryCount 表示当前是第几次投递（1-based）；首次投递为 1，第 1 次重试投递为 2。
	// 当处理失败且 DeliveryCount >= SubscribeOptions.MaxDeliveries 时，消息将进入 DLQ。
	DeliveryCount int

	// PublishedAt 表示消息发布时间。
	PublishedAt time.Time
}

// Handler 表示订阅消息处理函数。
// 返回 nil 表示处理成功并 ack；返回 error 表示处理失败并按 retry/DLQ 策略转发。
type Handler func(ctx context.Context, msg Message) error

// Subscription 表示已建立的 RabbitMQ 订阅。
type Subscription interface {
	// Drain 优雅停止订阅，并等待已投递消息处理完成。
	Drain(ctx context.Context) error

	// Close 立即停止订阅并释放相关资源。
	Close() error
}

// toMessage 把 AMQP delivery 转成对业务暴露的消息模型，并保留常用路由字段。
// 若消息经过 retry 死信回流导致 delivery.RoutingKey 变为队列名，则从 header 还原初始业务 routing key。
func toMessage(exchange, queue string, delivery amqp.Delivery) Message {
	headers := tableToHeaders(delivery.Headers)
	routingKey := originalRoutingKeyFromDelivery(delivery)
	messageID := strings.TrimSpace(delivery.MessageId)
	if messageID == "" {
		messageID = fmt.Sprintf("%s:%d", routingKey, delivery.DeliveryTag)
	}

	return Message{
		Exchange:      exchange,
		Queue:         queue,
		RoutingKey:    routingKey,
		Data:          bytes.Clone(delivery.Body),
		Headers:       headers,
		ID:            messageID,
		DeliveryCount: retryCountFromHeaders(delivery.Headers) + 1,
		PublishedAt:   delivery.Timestamp,
	}
}

// publishingFromDelivery 复制原消息属性、保留原始业务 routing key 并递增 retry count，用于失败后转发到 retry/DLQ。
func publishingFromDelivery(delivery amqp.Delivery, retryCount int) amqp.Publishing {
	headers := cloneTable(delivery.Headers)
	delete(headers, publishSeqHeader)
	headers[retryCountHeader] = int64(retryCount)
	if origRoutingKey := originalRoutingKeyFromDelivery(delivery); origRoutingKey != "" {
		headers[originalRoutingKeyHeader] = origRoutingKey
	}

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     delivery.ContentType,
		ContentEncoding: delivery.ContentEncoding,
		DeliveryMode:    amqp.Persistent,
		Priority:        delivery.Priority,
		CorrelationId:   delivery.CorrelationId,
		ReplyTo:         delivery.ReplyTo,
		MessageId:       delivery.MessageId,
		Timestamp:       delivery.Timestamp,
		Type:            delivery.Type,
		AppId:           delivery.AppId,
		Body:            bytes.Clone(delivery.Body),
	}
}

// originalRoutingKeyFromDelivery 返回消息的初始业务 routing key。
// 优先读取 x-mq-original-routing-key 头；不存在时回退为 delivery.RoutingKey。
func originalRoutingKeyFromDelivery(delivery amqp.Delivery) string {
	if delivery.Headers != nil {
		if value, ok := delivery.Headers[originalRoutingKeyHeader]; ok {
			switch typed := value.(type) {
			case string:
				if trimmed := strings.TrimSpace(typed); trimmed != "" {
					return trimmed
				}
			case []byte:
				if trimmed := strings.TrimSpace(string(typed)); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return delivery.RoutingKey
}

// isInternalHeader 判断是否为包内部使用的控制 header。
func isInternalHeader(key string) bool {
	return key == retryCountHeader || key == originalRoutingKeyHeader || key == publishSeqHeader
}

// headersToTable 把公共 string header 转成 AMQP table。
func headersToTable(headers map[string]string) amqp.Table {
	if len(headers) == 0 {
		return nil
	}
	table := make(amqp.Table, len(headers))
	for key, value := range headers {
		table[key] = value
	}
	return table
}

// tableToHeaders 把 AMQP table 转成公共 string header（自动过滤内部控制头），复杂类型按字符串表达。
func tableToHeaders(table amqp.Table) map[string]string {
	if len(table) == 0 {
		return nil
	}
	var headers map[string]string
	for key, value := range table {
		if isInternalHeader(key) {
			continue
		}
		if headers == nil {
			headers = make(map[string]string, len(table))
		}
		switch typed := value.(type) {
		case string:
			headers[key] = typed
		case []byte:
			headers[key] = string(typed)
		case fmt.Stringer:
			headers[key] = typed.String()
		default:
			headers[key] = fmt.Sprint(typed)
		}
	}
	return headers
}

// cloneTable 复制 AMQP table，避免修改原 delivery header。
func cloneTable(table amqp.Table) amqp.Table {
	cloned := make(amqp.Table, len(table)+1)
	maps.Copy(cloned, table)
	return cloned
}

// retryCountFromHeaders 兼容解析 RabbitMQ header 中的 retry count。
func retryCountFromHeaders(table amqp.Table) int {
	value, ok := table[retryCountHeader]
	if !ok {
		return 0
	}

	switch typed := value.(type) {
	case int:
		return typed
	case int8:
		return int(typed)
	case int16:
		return int(typed)
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case uint16:
		return int(typed)
	case uint32:
		return int(typed)
	case string:
		parsed, err := strconv.Atoi(typed)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

// publishSeqFromHeaders 解析内部发布序号 header，用于精确配对 mandatory basic.return。
func publishSeqFromHeaders(table amqp.Table) (uint64, bool) {
	if len(table) == 0 {
		return 0, false
	}
	value, ok := table[publishSeqHeader]
	if !ok {
		return 0, false
	}

	switch typed := value.(type) {
	case uint64:
		return typed, typed > 0
	case int64:
		if typed <= 0 {
			return 0, false
		}
		return uint64(typed), true
	case int:
		if typed <= 0 {
			return 0, false
		}
		return uint64(typed), true
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(typed), 10, 64)
		if err != nil || parsed == 0 {
			return 0, false
		}
		return parsed, true
	case []byte:
		parsed, err := strconv.ParseUint(strings.TrimSpace(string(typed)), 10, 64)
		if err != nil || parsed == 0 {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}
