package mq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// TestBuildPublishOptions 验证发布选项构造逻辑。
func TestBuildPublishOptions(t *testing.T) {
	tests := []struct {
		name   string
		opts   []PublishOption
		verify func(t *testing.T, got PublishOptions)
	}{
		{
			name: "defaults",
			verify: func(t *testing.T, got PublishOptions) {
				require.Empty(t, got.MessageID)
				require.Empty(t, got.Headers)
				require.True(t, got.Mandatory)
			},
		},
		{
			name: "message_id_headers_and_mandatory",
			opts: []PublishOption{
				WithMessageID("msg-1"),
				WithHeader("trace_id", "trace-1"),
				WithHeaders(map[string]string{"event_type": "order.created"}),
				WithMandatory(false),
			},
			verify: func(t *testing.T, got PublishOptions) {
				require.Equal(t, "msg-1", got.MessageID)
				require.Equal(t, "trace-1", got.Headers["trace_id"])
				require.Equal(t, "order.created", got.Headers["event_type"])
				require.False(t, got.Mandatory)
			},
		},
		{
			name: "nil_option_ignored",
			opts: []PublishOption{nil, WithHeader("request_id", "req-1")},
			verify: func(t *testing.T, got PublishOptions) {
				require.Equal(t, "req-1", got.Headers["request_id"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildPublishOptions(tt.opts...)
			tt.verify(t, got)
		})
	}
}

// TestNormalizeSubscribeOptions 验证订阅参数默认值与非法输入校验。
func TestNormalizeSubscribeOptions(t *testing.T) {
	tests := []struct {
		name    string
		input   SubscribeOptions
		wantErr string
		verify  func(t *testing.T, got SubscribeOptions)
	}{
		{
			name: "defaults_applied",
			input: SubscribeOptions{
				Queue:       "example-service.order-created",
				BindingKeys: []string{"test.order.created"},
			},
			verify: func(t *testing.T, got SubscribeOptions) {
				require.Equal(t, []string{"test.order.created"}, got.BindingKeys)
				require.Equal(t, DefaultPrefetch, got.Prefetch)
				require.Equal(t, DefaultConcurrency, got.Concurrency)
				require.Equal(t, DefaultMaxDeliveries, got.MaxDeliveries)
				require.Equal(t, DefaultRetryDelay, got.RetryDelay)
			},
		},
		{
			name: "custom_values_preserved",
			input: SubscribeOptions{
				Queue:         "example-service",
				BindingKeys:   []string{"test.order.*", "test.payment.#"},
				Prefetch:      64,
				Concurrency:   8,
				MaxDeliveries: 9,
				RetryDelay:    3 * time.Second,
			},
			verify: func(t *testing.T, got SubscribeOptions) {
				require.Equal(t, []string{"test.order.*", "test.payment.#"}, got.BindingKeys)
				require.Equal(t, 64, got.Prefetch)
				require.Equal(t, 8, got.Concurrency)
				require.Equal(t, 9, got.MaxDeliveries)
				require.Equal(t, 3*time.Second, got.RetryDelay)
			},
		},
		{
			name: "prefetch_elevated_to_concurrency_when_smaller",
			input: SubscribeOptions{
				Queue:       "example-service",
				BindingKeys: []string{"test.order.created"},
				Prefetch:    4,
				Concurrency: 16,
			},
			verify: func(t *testing.T, got SubscribeOptions) {
				require.Equal(t, 16, got.Prefetch)
				require.Equal(t, 16, got.Concurrency)
			},
		},
		{
			name:    "missing_queue",
			input:   SubscribeOptions{},
			wantErr: "queue is required",
		},
		{
			name: "missing_binding_keys",
			input: SubscribeOptions{
				Queue: "example-service",
			},
			wantErr: "binding keys are required",
		},
		{
			name: "empty_binding_key",
			input: SubscribeOptions{
				Queue:       "example-service",
				BindingKeys: []string{"test.order.*", " "},
			},
			wantErr: "binding key at index 1 is empty",
		},
		{
			name: "unsupported_legacy_wildcard_rejected",
			input: SubscribeOptions{
				Queue:       "example-service",
				BindingKeys: []string{"test.>"},
			},
			wantErr: "unsupported wildcard",
		},
		{
			name: "empty_binding_word_rejected",
			input: SubscribeOptions{
				Queue:       "example-service",
				BindingKeys: []string{"test..key"},
			},
			wantErr: "contains empty word",
		},
		{
			name: "wildcard_must_be_standalone_word",
			input: SubscribeOptions{
				Queue:       "example-service",
				BindingKeys: []string{"test.order*"},
			},
			wantErr: "invalid wildcard word",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeSubscribeOptions(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			tt.verify(t, got)
		})
	}
}

// TestNormalizeConfig 验证客户端配置默认值与必填校验。
func TestNormalizeConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
		verify  func(t *testing.T, got Config)
	}{
		{name: "missing_url", cfg: Config{Exchange: "test.events"}, wantErr: "url is required"},
		{name: "missing_exchange", cfg: Config{URL: "amqp://guest:guest@127.0.0.1:5672/"}, wantErr: "exchange is required"},
		{
			name: "defaults_applied",
			cfg: Config{
				URL:      "amqp://guest:guest@127.0.0.1:5672/",
				Exchange: "test.events",
			},
			verify: func(t *testing.T, got Config) {
				require.Equal(t, DefaultExchangeType, got.ExchangeType)
				require.Equal(t, DefaultConnectTimeout, got.ConnectTimeout)
				require.Equal(t, DefaultPublishTimeout, got.PublishTimeout)
				require.Equal(t, DefaultCloseTimeout, got.CloseTimeout)
				require.NotNil(t, got.Logger)
			},
		},
		{
			name: "custom_values_preserved",
			cfg: Config{
				URL:            "amqp://guest:guest@127.0.0.1:5672/",
				Exchange:       "test.events",
				ExchangeType:   "direct",
				ConnectTimeout: time.Second,
				PublishTimeout: 2 * time.Second,
				CloseTimeout:   3 * time.Second,
				Logger:         &fakeLogger{},
			},
			verify: func(t *testing.T, got Config) {
				require.Equal(t, "direct", got.ExchangeType)
				require.Equal(t, time.Second, got.ConnectTimeout)
				require.Equal(t, 2*time.Second, got.PublishTimeout)
				require.Equal(t, 3*time.Second, got.CloseTimeout)
				require.IsType(t, &fakeLogger{}, got.Logger)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeConfig(tt.cfg)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			tt.verify(t, got)
		})
	}
}

// TestPublishDoesNotSpamInfofOnSuccessHotPath 验证单条消息发布成功热路径不打印 Infof 日志，避免高吞吐下 I/O 放大。
func TestPublishDoesNotSpamInfofOnSuccessHotPath(t *testing.T) {
	logger := &fakeLogger{}
	client := &Client{
		exchange:  "test.events",
		name:      "mq-test",
		log:       logger,
		publisher: &stubPublisher{},
	}

	err := client.Publish(context.Background(), "test.order.created", []byte("payload"), WithMessageID("msg-1"))

	require.NoError(t, err)
	require.Empty(t, logger.infos)
}

// TestRoutingKeyValidation 验证发布 routing key 不允许使用通配符。
func TestRoutingKeyValidation(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr string
	}{
		{name: "valid", input: "test.order.created", want: "test.order.created"},
		{name: "trim", input: " test.order.deleted ", want: "test.order.deleted"},
		{name: "empty", input: " ", wantErr: "routing key is required"},
		{name: "star", input: "test.order.*", wantErr: "must not contain wildcard"},
		{name: "hash", input: "test.#", wantErr: "must not contain wildcard"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizePublishRoutingKey(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestTopologyNames 验证 retry 和 DLQ 拓扑命名规则。
func TestTopologyNames(t *testing.T) {
	require.Equal(t, "test.events.retry", retryExchangeName("test.events"))
	require.Equal(t, "test.events.dlx", deadLetterExchangeName("test.events"))
	require.Equal(t, "example-service.retry.1500", retryQueueName("example-service", 1500*time.Millisecond))
	require.Equal(t, "example-service.dlq", dlqName("example-service"))
}

// TestTTLMilliseconds 验证延迟重试 TTL 取值。
func TestTTLMilliseconds(t *testing.T) {
	got, err := ttlMilliseconds(1500 * time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, int32(1500), got)

	_, err = ttlMilliseconds(0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least 1ms")

	_, err = ttlMilliseconds(500 * time.Microsecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least 1ms")
}

// TestRetryCountFromHeaders 验证 retry count header 的兼容解析。
func TestRetryCountFromHeaders(t *testing.T) {
	tests := []struct {
		name string
		in   amqp.Table
		want int
	}{
		{name: "missing", in: amqp.Table{}, want: 0},
		{name: "int32", in: amqp.Table{retryCountHeader: int32(2)}, want: 2},
		{name: "int64", in: amqp.Table{retryCountHeader: int64(3)}, want: 3},
		{name: "string", in: amqp.Table{retryCountHeader: "4"}, want: 4},
		{name: "bad_string", in: amqp.Table{retryCountHeader: "bad"}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, retryCountFromHeaders(tt.in))
		})
	}
}

// TestToMessage 验证 AMQP delivery 到通用消息模型的转换、内部控制头过滤与 Body 深拷贝隔离。
func TestToMessage(t *testing.T) {
	publishedAt := time.Unix(1710000000, 0)
	rawBody := []byte("payload")
	got := toMessage("test.events", "example-service", amqp.Delivery{
		RoutingKey:  "test.order.created",
		Body:        rawBody,
		MessageId:   "msg-1",
		Timestamp:   publishedAt,
		DeliveryTag: 9,
		Headers: amqp.Table{
			"trace_id":               "trace-1",
			"raw":                    []byte("bytes"),
			"attempt":                int32(2),
			retryCountHeader:         int64(3),
			originalRoutingKeyHeader: "test.order.created",
			publishSeqHeader:         "99",
		},
	})

	require.Equal(t, "test.events", got.Exchange)
	require.Equal(t, "example-service", got.Queue)
	require.Equal(t, "test.order.created", got.RoutingKey)
	require.Equal(t, []byte("payload"), got.Data)
	require.Equal(t, "msg-1", got.ID)
	require.Equal(t, 4, got.DeliveryCount)
	require.Equal(t, "trace-1", got.Headers["trace_id"])
	require.Equal(t, "bytes", got.Headers["raw"])
	require.Equal(t, "2", got.Headers["attempt"])
	require.NotContains(t, got.Headers, retryCountHeader)
	require.NotContains(t, got.Headers, originalRoutingKeyHeader)
	require.NotContains(t, got.Headers, publishSeqHeader)
	require.Equal(t, publishedAt, got.PublishedAt)

	got.Data[0] = 'X'
	require.Equal(t, byte('p'), rawBody[0], "mutating Message.Data must not mutate delivery.Body")
}

// TestToMessageIDFallback 验证无 message id 时使用 routing key 和 delivery tag 作为 fallback。
func TestToMessageIDFallback(t *testing.T) {
	got := toMessage("test.events", "example-service", amqp.Delivery{
		RoutingKey:  "test.order.created",
		DeliveryTag: 42,
	})
	require.Equal(t, "test.order.created:42", got.ID)
	require.Equal(t, 1, got.DeliveryCount)
}

// TestPublishReturnCorrelation 验证 mandatory return 按发布序号精确配对，且已超时的旧 return 不会串扰后续请求。
func TestPublishReturnCorrelation(t *testing.T) {
	returns := make(chan amqp.Return, 4)
	publisher := &amqpPublisher{
		returns: returns,
		waiters: make(map[uint64]chan amqp.Return),
	}

	// 模拟请求 1 注册后因超时注销，但其 basic.return 随后才迟到
	seq1, _ := publisher.registerReturnWaiter()
	publisher.unregisterReturnWaiter(seq1)
	returns <- amqp.Return{
		Exchange:   "test.events",
		RoutingKey: "test.order.stale",
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE_STALE",
		Headers:    amqp.Table{publishSeqHeader: fmt.Sprintf("%d", seq1)},
	}

	// 请求 2 正常注册并成功路由（无自己的 return），不应误读请求 1 迟到的 return
	seq2, retCh2 := publisher.registerReturnWaiter()
	require.Nil(t, publisher.takeReturn(retCh2))
	publisher.unregisterReturnWaiter(seq2)

	// 请求 3 产生自己的 basic.return，应精确命中
	seq3, retCh3 := publisher.registerReturnWaiter()
	returns <- amqp.Return{
		Exchange:   "test.events",
		RoutingKey: "test.order.missing",
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE_3",
		Headers:    amqp.Table{publishSeqHeader: fmt.Sprintf("%d", seq3)},
	}
	got3 := publisher.takeReturn(retCh3)
	require.NotNil(t, got3)
	require.Equal(t, "NO_ROUTE_3", got3.ReplyText)
	require.Contains(t, publishReturnError(*got3).Error(), "message returned by broker")
	publisher.unregisterReturnWaiter(seq3)

	// returns 通道关闭时应返回明确错误
	closedReturns := make(chan amqp.Return)
	close(closedReturns)
	closedPublisher := &amqpPublisher{
		returns: closedReturns,
		waiters: make(map[uint64]chan amqp.Return),
	}
	_, closedWaiter := closedPublisher.registerReturnWaiter()
	closedReturn := closedPublisher.takeReturn(closedWaiter)
	require.NotNil(t, closedReturn)
	require.Contains(t, publishReturnError(*closedReturn).Error(), "return channel closed")
}

// TestHandleDeliverySuccessAck 验证 handler 成功时会手动 ack。
func TestHandleDeliverySuccessAck(t *testing.T) {
	client := &Client{exchange: "test.events"}
	acker := &spyAcknowledger{}

	err := client.handleDelivery(context.Background(), SubscribeOptions{
		BindingKeys:   []string{"test.order.created"},
		Queue:         "example-service",
		MaxDeliveries: 2,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  7,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}, func(context.Context, Message) error {
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), acker.ackTag)
	require.False(t, acker.nackCalled)
}

// TestHandleDeliveryContextCanceledNacks 验证 handler 失败且上下文取消时不会进入 retry，而是重新入队。
func TestHandleDeliveryContextCanceledNacks(t *testing.T) {
	client := &Client{exchange: "test.events"}
	acker := &spyAcknowledger{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.handleDelivery(ctx, SubscribeOptions{
		BindingKeys:   []string{"test.order.created"},
		Queue:         "example-service",
		MaxDeliveries: 2,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  11,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}, func(context.Context, Message) error {
		return errors.New("shutdown")
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "canceled")
	require.True(t, acker.nackCalled)
	require.Equal(t, uint64(11), acker.nackTag)
	require.True(t, acker.requeue)
	require.False(t, acker.ackCalled)
}

// TestConsumeLoopUsesWarnfLogger 验证未达 MaxDeliveries、转发 retry 成功时记录 Warnf。
func TestConsumeLoopUsesWarnfLogger(t *testing.T) {
	logger := &fakeLogger{}
	client := &Client{
		exchange:  "test.events",
		log:       logger,
		publisher: &stubPublisher{},
		subs:      make(map[*subscription]struct{}),
	}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: &spyAcknowledger{},
		DeliveryTag:  12,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}
	close(deliveries)

	sub := &subscription{done: make(chan struct{})}
	client.consumeLoop(context.Background(), sub, SubscribeOptions{
		Queue:         "example-service",
		BindingKeys:   []string{"test.order.created"},
		MaxDeliveries: 3,
	}, deliveries, func(context.Context, Message) error {
		return errors.New("boom")
	})

	require.Empty(t, logger.errors)
	require.Len(t, logger.warns, 1)
	require.Contains(t, logger.warns[0], "action=handle_message")
	require.Contains(t, logger.warns[0], "routing_key=test.order.created")
}

// TestConsumeLoopUsesErrorfLogger 验证进入 DLQ 或转发失败时记录 Errorf。
func TestConsumeLoopUsesErrorfLogger(t *testing.T) {
	logger := &fakeLogger{}
	client := &Client{
		exchange:  "test.events",
		log:       logger,
		publisher: &stubPublisher{},
		subs:      make(map[*subscription]struct{}),
	}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: &spyAcknowledger{},
		DeliveryTag:  13,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-dlq",
	}
	close(deliveries)

	sub := &subscription{done: make(chan struct{})}
	client.consumeLoop(context.Background(), sub, SubscribeOptions{
		Queue:         "example-service",
		BindingKeys:   []string{"test.order.created"},
		MaxDeliveries: 1,
	}, deliveries, func(context.Context, Message) error {
		return errors.New("boom")
	})

	require.Empty(t, logger.warns)
	require.Len(t, logger.errors, 1)
	require.Contains(t, logger.errors[0], "action=handle_message")
	require.Contains(t, logger.errors[0], "routing_key=test.order.created")

	logger = &fakeLogger{}
	client = &Client{
		exchange:  "test.events",
		log:       logger,
		publisher: &stubPublisher{err: errors.New("publish failed")},
		subs:      make(map[*subscription]struct{}),
	}
	deliveries = make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: &spyAcknowledger{},
		DeliveryTag:  14,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-forward",
	}
	close(deliveries)

	sub = &subscription{done: make(chan struct{})}
	client.consumeLoop(context.Background(), sub, SubscribeOptions{
		Queue:         "example-service",
		BindingKeys:   []string{"test.order.created"},
		MaxDeliveries: 3,
	}, deliveries, func(context.Context, Message) error {
		return errors.New("boom")
	})

	require.Empty(t, logger.warns)
	require.Len(t, logger.errors, 1)
	require.Contains(t, logger.errors[0], "publish failure target failed")
}

// TestSubscriptionCloseWaitsForConsumeLoop 验证订阅关闭先取消 consumer，再等待消费循环释放 channel。
func TestSubscriptionCloseWaitsForConsumeLoop(t *testing.T) {
	channel := &spySubscriptionChannel{}
	done := make(chan struct{})
	close(done)
	sub := &subscription{
		channel:      channel,
		consumerTag:  "consumer-1",
		done:         done,
		closeTimeout: time.Second,
	}

	require.NoError(t, sub.Close())
	require.True(t, channel.cancelCalled)
	require.False(t, channel.closeCalled)
}

// TestSubscriptionCloseClosesChannelOnTimeout 验证消费循环未退出时关闭订阅会兜底释放 channel。
func TestSubscriptionCloseClosesChannelOnTimeout(t *testing.T) {
	channel := &spySubscriptionChannel{}
	sub := &subscription{
		channel:      channel,
		consumerTag:  "consumer-1",
		done:         make(chan struct{}),
		closeTimeout: time.Millisecond,
	}

	err := sub.Close()

	require.Error(t, err)
	require.Contains(t, err.Error(), "wait done timeout")
	require.True(t, channel.cancelCalled)
	require.True(t, channel.closeCalled)
}

// TestLockedAcknowledgerUsesSubscriptionChannelLock 验证消息确认与 consumer cancel 使用同一把锁。
func TestLockedAcknowledgerUsesSubscriptionChannelLock(t *testing.T) {
	sub := &subscription{}
	acker := &spyAcknowledger{}
	delivery := sub.lockDeliveryAcknowledger(amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  13,
	})

	sub.channelMu.Lock()
	ackDone := make(chan error, 1)
	go func() {
		ackDone <- delivery.Ack(false)
	}()

	select {
	case err := <-ackDone:
		t.Fatalf("ack completed before channel lock released: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	sub.channelMu.Unlock()
	require.NoError(t, <-ackDone)
	require.True(t, acker.ackCalled)
	require.Equal(t, uint64(13), acker.ackTag)
}

// TestHandleDeliveryFailurePublishesRetry 验证首次失败时按队列名点对点发布到 retry exchange，并保留原始业务 routing key。
func TestHandleDeliveryFailurePublishesRetry(t *testing.T) {
	publisher := &stubPublisher{}
	client := &Client{exchange: "test.events", publisher: publisher}
	acker := &spyAcknowledger{}

	err := client.handleDelivery(context.Background(), SubscribeOptions{
		BindingKeys:   []string{"test.order.created"},
		Queue:         "example-service",
		MaxDeliveries: 3,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  8,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
		Body:         []byte("payload"),
	}, func(context.Context, Message) error {
		return errors.New("boom")
	})

	require.Error(t, err)
	require.Len(t, publisher.published, 1)
	require.Equal(t, "test.events.retry", publisher.published[0].exchange)
	require.Equal(t, "example-service", publisher.published[0].routingKey)
	require.True(t, publisher.published[0].mandatory)
	require.Equal(t, int64(1), publisher.published[0].publishing.Headers[retryCountHeader])
	require.Equal(t, "test.order.created", publisher.published[0].publishing.Headers[originalRoutingKeyHeader])
	require.Equal(t, uint64(8), acker.ackTag)
	require.False(t, acker.nackCalled)
}

// TestHandleDeliveryFailurePublishesDLQ 验证达到最大投递次数时按队列名点对点发布到 DLX，并保留原始业务 routing key。
func TestHandleDeliveryFailurePublishesDLQ(t *testing.T) {
	publisher := &stubPublisher{}
	client := &Client{exchange: "test.events", publisher: publisher}
	acker := &spyAcknowledger{}

	err := client.handleDelivery(context.Background(), SubscribeOptions{
		BindingKeys:   []string{"test.order.created"},
		Queue:         "example-service",
		MaxDeliveries: 2,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  9,
		RoutingKey:   "example-service",
		MessageId:    "msg-1",
		Headers: amqp.Table{
			retryCountHeader:         int32(1),
			originalRoutingKeyHeader: "test.order.created",
		},
	}, func(_ context.Context, msg Message) error {
		require.Equal(t, "test.order.created", msg.RoutingKey)
		return errors.New("boom")
	})

	require.Error(t, err)
	require.Len(t, publisher.published, 1)
	require.Equal(t, "test.events.dlx", publisher.published[0].exchange)
	require.Equal(t, "example-service", publisher.published[0].routingKey)
	require.Equal(t, int64(2), publisher.published[0].publishing.Headers[retryCountHeader])
	require.Equal(t, "test.order.created", publisher.published[0].publishing.Headers[originalRoutingKeyHeader])
	require.Equal(t, uint64(9), acker.ackTag)
}

// TestHandleDeliveryFailureNacksWhenRetryPublishFails 验证转发失败时原消息会重新入队。
func TestHandleDeliveryFailureNacksWhenRetryPublishFails(t *testing.T) {
	client := &Client{exchange: "test.events", publisher: &stubPublisher{err: errors.New("publish failed")}}
	acker := &spyAcknowledger{}

	err := client.handleDelivery(context.Background(), SubscribeOptions{
		BindingKeys:   []string{"test.order.created"},
		Queue:         "example-service",
		MaxDeliveries: 2,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  10,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}, func(context.Context, Message) error {
		return errors.New("boom")
	})

	require.Error(t, err)
	require.True(t, acker.nackCalled)
	require.Equal(t, uint64(10), acker.nackTag)
	require.True(t, acker.requeue)
	require.False(t, acker.ackCalled)
}

// TestClosedClientRejectsOperations 验证关闭状态会拒绝新操作。
func TestClosedClientRejectsOperations(t *testing.T) {
	client := &Client{closed: true, exchange: "test.events"}

	err := client.EnsureTopology(context.Background(), SubscribeOptions{
		BindingKeys: []string{"test.order.created"},
		Queue:       "example-service",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "client already closed")

	err = client.Publish(context.Background(), "test.order.created", []byte("payload"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "client already closed")

	sub, err := client.Subscribe(context.Background(), SubscribeOptions{
		BindingKeys: []string{"test.order.created"},
		Queue:       "example-service",
	}, func(context.Context, Message) error {
		return nil
	})
	require.Nil(t, sub)
	require.Error(t, err)
	require.Contains(t, err.Error(), "client already closed")
}

// TestUninitializedClientRejectsOperations 验证零值客户端会返回明确错误。
func TestUninitializedClientRejectsOperations(t *testing.T) {
	client := &Client{exchange: "test.events"}

	err := client.EnsureTopology(context.Background(), SubscribeOptions{
		BindingKeys: []string{"test.order.created"},
		Queue:       "example-service",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "client is not initialized")

	err = client.Publish(context.Background(), "test.order.created", []byte("payload"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "client is not initialized")

	sub, err := client.Subscribe(context.Background(), SubscribeOptions{
		BindingKeys: []string{"test.order.created"},
		Queue:       "example-service",
	}, func(context.Context, Message) error {
		return nil
	})
	require.Nil(t, sub)
	require.Error(t, err)
	require.Contains(t, err.Error(), "client is not initialized")
}

type publishedMessage struct {
	exchange   string
	routingKey string
	publishing amqp.Publishing
	mandatory  bool
}

type stubPublisher struct {
	published []publishedMessage
	err       error
}

func (s *stubPublisher) Publish(_ context.Context, exchange, routingKey string, publishing amqp.Publishing, mandatory bool) error {
	s.published = append(s.published, publishedMessage{
		exchange:   exchange,
		routingKey: routingKey,
		publishing: publishing,
		mandatory:  mandatory,
	})
	return s.err
}

type fakeLogger struct {
	infos  []string
	warns  []string
	errors []string
}

var _ Logger = (*fakeLogger)(nil)

func (f *fakeLogger) Infof(format string, args ...any) {
	f.infos = append(f.infos, formatLog(format, args...))
}

func (f *fakeLogger) Warnf(format string, args ...any) {
	f.warns = append(f.warns, formatLog(format, args...))
}

func (f *fakeLogger) Errorf(format string, args ...any) {
	f.errors = append(f.errors, formatLog(format, args...))
}

func formatLog(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}

type spyAcknowledger struct {
	ackCalled  bool
	ackTag     uint64
	nackCalled bool
	nackTag    uint64
	requeue    bool
}

func (s *spyAcknowledger) Ack(tag uint64, multiple bool) error {
	s.ackCalled = true
	s.ackTag = tag
	return nil
}

func (s *spyAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	s.nackCalled = true
	s.nackTag = tag
	s.requeue = requeue
	return nil
}

func (s *spyAcknowledger) Reject(uint64, bool) error {
	return nil
}

type spySubscriptionChannel struct {
	cancelCalled bool
	cancelTag    string
	closeCalled  bool
	cancelErr    error
	closeErr     error
}

func (s *spySubscriptionChannel) Cancel(consumer string, _ bool) error {
	s.cancelCalled = true
	s.cancelTag = consumer
	return s.cancelErr
}

func (s *spySubscriptionChannel) Close() error {
	s.closeCalled = true
	return s.closeErr
}

// TestIsTransportFailure 验证连接/channel 关闭类错误识别。
func TestIsTransportFailure(t *testing.T) {
	require.True(t, isTransportFailure(amqp.ErrClosed))
	require.True(t, isTransportFailure(fmt.Errorf("wrap: %w", amqp.ErrClosed)))
	require.True(t, isTransportFailure(&amqp.Error{Code: 504, Reason: "channel/connection is not open"}))
	require.True(t, isTransportFailure(errors.New(`Exception (504) Reason: "channel/connection is not open"`)))
	require.False(t, isTransportFailure(errors.New("message nacked by broker")))
}

// TestPublishOutcomeErrorSubmittedSemantics 验证已提交消息不会被自动重发。
func TestPublishOutcomeErrorSubmittedSemantics(t *testing.T) {
	publisher := &sequencePublisher{errs: []error{
		&publishOutcomeError{err: amqp.ErrClosed, submitted: true},
	}}
	client := &Client{
		exchange:  "test.events",
		publisher: publisher,
	}

	err := client.publishMessage(context.Background(), "test.events", "test.key", amqp.Publishing{Body: []byte("x")}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "delivery outcome unknown")
	require.Equal(t, 1, publisher.calls)
}

// TestPublishRetriesWhenNotSubmitted 验证请求未发出时的关闭错误会重试一次。
func TestPublishRetriesWhenNotSubmitted(t *testing.T) {
	publisher := &sequencePublisher{errs: []error{
		&publishOutcomeError{err: amqp.ErrClosed, submitted: false},
		nil,
	}}
	client := &Client{
		exchange:  "test.events",
		publisher: publisher,
	}

	err := client.publishMessage(context.Background(), "test.events", "test.key", amqp.Publishing{Body: []byte("x")}, true)
	require.NoError(t, err)
	require.Equal(t, 2, publisher.calls)
}

// TestPublishDoesNotRetryOnNonTransportError 验证非传输层错误不会重试。
func TestPublishDoesNotRetryOnNonTransportError(t *testing.T) {
	publisher := &sequencePublisher{errs: []error{errors.New("boom")}}
	client := &Client{exchange: "test.events", publisher: publisher}

	err := client.publishMessage(context.Background(), "test.events", "test.key", amqp.Publishing{Body: []byte("x")}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
	require.Equal(t, 1, publisher.calls)
}

// TestEnsureConnectedRejectsAfterClose 验证 Close 后不再重连。
func TestEnsureConnectedRejectsAfterClose(t *testing.T) {
	lifeCtx, cancel := context.WithCancel(context.Background())
	client := &Client{
		cfg:        Config{URL: "amqp://guest:guest@127.0.0.1:5672/"},
		lifeCtx:    lifeCtx,
		lifeCancel: cancel,
		log:        &fakeLogger{},
		subs:       make(map[*subscription]struct{}),
		broken:     true,
	}
	require.NoError(t, client.Close())

	err := client.ensureConnected(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "client already closed")
}

// TestBeginOrJoinReconnectSingleflight 验证并发 ensure 只会启动一次重连等待通道。
func TestBeginOrJoinReconnectSingleflight(t *testing.T) {
	client := &Client{
		cfg:     Config{URL: "amqp://guest:guest@127.0.0.1:5672/"},
		lifeCtx: t.Context(),
		log:     &fakeLogger{},
		broken:  true,
	}

	wait1, err := client.beginOrJoinReconnect()
	require.NoError(t, err)
	require.Nil(t, wait1)

	wait2, err := client.beginOrJoinReconnect()
	require.NoError(t, err)
	require.NotNil(t, wait2)

	finished := make(chan struct{})
	go func() {
		<-wait2
		close(finished)
	}()

	client.finishReconnect(nil)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("waiters were not released")
	}
}

// TestReconnectDelayWithJitter 验证退避抖动不会低于基准间隔。
func TestReconnectDelayWithJitter(t *testing.T) {
	base := 200 * time.Millisecond
	for range 20 {
		got := reconnectDelayWithJitter(base)
		require.GreaterOrEqual(t, got, base)
		require.LessOrEqual(t, got, base+base/5+time.Millisecond)
	}
}

// TestMarkStoppedDoesNotCancelRunningContext 验证 Drain 使用的 markStopped 不会取消 handler context。
func TestMarkStoppedDoesNotCancelRunningContext(t *testing.T) {
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	loopCtx, loopCancel := context.WithCancel(runCtx)

	sub := &subscription{
		parentCtx:  runCtx,
		runCancel:  runCancel,
		loopCancel: loopCancel,
		stopCh:     make(chan struct{}),
	}

	sub.markStopped()
	require.True(t, sub.isStopped())
	require.NoError(t, runCtx.Err())
	require.NoError(t, loopCtx.Err())
	select {
	case <-sub.stopCh:
	default:
		t.Fatal("stopCh should be closed after markStopped")
	}

	sub.cancelRunning()
	require.Error(t, runCtx.Err())
	require.Error(t, loopCtx.Err())
}

// TestDrainDoesNotCancelInFlightHandler 验证 Drain 等待 handler 完成且不取消其 context。
func TestDrainDoesNotCancelInFlightHandler(t *testing.T) {
	logger := &fakeLogger{}
	client := &Client{
		exchange: "test.events",
		log:      logger,
		subs:     make(map[*subscription]struct{}),
		lifeCtx:  t.Context(),
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	handlerStarted := make(chan struct{}, 1)
	releaseHandler := make(chan struct{})
	var canceledDuringHandler atomic.Bool
	sub := &subscription{
		client:    client,
		opts:      SubscribeOptions{Queue: "example-service", BindingKeys: []string{"test.order.created"}, MaxDeliveries: 2},
		parentCtx: runCtx,
		runCancel: runCancel,
		handler: func(ctx context.Context, _ Message) error {
			handlerStarted <- struct{}{}
			<-releaseHandler
			if ctx.Err() != nil {
				canceledDuringHandler.Store(true)
			}
			return nil
		},
		consumerTag:  "example-service",
		done:         make(chan struct{}),
		stopCh:       make(chan struct{}),
		closeTimeout: time.Second,
		channel:      &spySubscriptionChannel{},
	}

	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: &spyAcknowledger{},
		DeliveryTag:  1,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-1",
	}

	client.mu.Lock()
	client.subs[sub] = struct{}{}
	client.mu.Unlock()
	go client.runSubscription(sub, deliveries)

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	drainErr := make(chan error, 1)
	go func() {
		drainErr <- sub.Drain(context.Background())
	}()

	time.Sleep(50 * time.Millisecond)
	require.False(t, canceledDuringHandler.Load(), "Drain must not cancel in-flight handler context")

	close(releaseHandler)
	close(deliveries)

	select {
	case err := <-drainErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not finish after handler completed")
	}
	require.False(t, canceledDuringHandler.Load(), "handler context was canceled during Drain")
}

// TestCloseCancelsInFlightHandler 验证 Close 会取消正在执行的 handler context。
func TestCloseCancelsInFlightHandler(t *testing.T) {
	client := &Client{
		exchange: "test.events",
		log:      &fakeLogger{},
		subs:     make(map[*subscription]struct{}),
		lifeCtx:  t.Context(),
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	handlerStarted := make(chan context.Context, 1)
	sub := &subscription{
		client:    client,
		opts:      SubscribeOptions{Queue: "example-service", BindingKeys: []string{"test.order.created"}, MaxDeliveries: 2},
		parentCtx: runCtx,
		runCancel: runCancel,
		handler: func(ctx context.Context, _ Message) error {
			handlerStarted <- ctx
			<-ctx.Done()
			return ctx.Err()
		},
		consumerTag:  "example-service",
		done:         make(chan struct{}),
		stopCh:       make(chan struct{}),
		closeTimeout: time.Second,
		channel:      &spySubscriptionChannel{},
	}

	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: &spyAcknowledger{},
		DeliveryTag:  2,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-2",
	}

	client.mu.Lock()
	client.subs[sub] = struct{}{}
	client.mu.Unlock()
	go client.runSubscription(sub, deliveries)

	var handlerCtx context.Context
	select {
	case handlerCtx = <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	require.NoError(t, sub.Close())
	select {
	case <-handlerCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel in-flight handler context")
	}
}

// TestSubscribeStartFailureDoesNotRegisterSubscription 验证首次建连失败时不会注册订阅或启动恢复。
func TestSubscribeStartFailureDoesNotRegisterSubscription(t *testing.T) {
	origDial := dialAMQP
	t.Cleanup(func() { dialAMQP = origDial })
	dialAMQP = func(string, amqp.Config) (*amqp.Connection, error) {
		return nil, errors.New("dial unavailable")
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()
	client := &Client{
		cfg: Config{
			URL:          "amqp://guest:guest@127.0.0.1:5672/",
			Exchange:     "test.events",
			ExchangeType: DefaultExchangeType,
		},
		exchange:       "test.events",
		exchangeType:   DefaultExchangeType,
		publishTimeout: DefaultPublishTimeout,
		log:            &fakeLogger{},
		lifeCtx:        lifeCtx,
		lifeCancel:     lifeCancel,
		subs:           make(map[*subscription]struct{}),
		broken:         true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	sub, err := client.Subscribe(ctx, SubscribeOptions{
		BindingKeys: []string{"test.order.created"},
		Queue:       "example-service",
	}, func(context.Context, Message) error { return nil })

	require.Error(t, err)
	require.Nil(t, sub)
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Empty(t, client.subs)
}

type sequencePublisher struct {
	errs  []error
	calls int
}

func (s *sequencePublisher) Publish(_ context.Context, _ string, _ string, _ amqp.Publishing, _ bool) error {
	idx := s.calls
	s.calls++
	if idx >= len(s.errs) {
		return nil
	}
	return s.errs[idx]
}

// TestToMessageRestoresOriginalRoutingKey 验证死信回流覆盖 delivery.RoutingKey 后仍能从 header 还原初始业务 routing key。
func TestToMessageRestoresOriginalRoutingKey(t *testing.T) {
	got := toMessage("test.events", "example-service", amqp.Delivery{
		RoutingKey:  "example-service",
		DeliveryTag: 15,
		Headers: amqp.Table{
			originalRoutingKeyHeader: "test.order.created",
		},
	})
	require.Equal(t, "test.order.created", got.RoutingKey)
	require.Equal(t, "test.order.created:15", got.ID)
}

// TestConsumeLoopConcurrentWorkers 验证 Concurrency > 1 时可并发执行 handler 并在退出前等待全部完成。
func TestConsumeLoopConcurrentWorkers(t *testing.T) {
	client := &Client{
		exchange: "test.events",
		log:      &fakeLogger{},
	}

	const workers = 4
	deliveries := make(chan amqp.Delivery, workers)
	for tag := uint64(1); tag <= uint64(workers); tag++ {
		deliveries <- amqp.Delivery{
			Acknowledger: &spyAcknowledger{},
			DeliveryTag:  tag,
			RoutingKey:   "test.order.created",
			MessageId:    fmt.Sprintf("msg-%d", tag),
		}
	}
	close(deliveries)

	var active atomic.Int32
	var maxActive atomic.Int32
	var processed atomic.Int32
	allRunning := make(chan struct{})

	sub := &subscription{done: make(chan struct{})}
	client.consumeLoop(context.Background(), sub, SubscribeOptions{
		BindingKeys:   []string{"test.order.created"},
		Queue:         "example-service",
		Concurrency:   workers,
		MaxDeliveries: 2,
	}, deliveries, func(context.Context, Message) error {
		cur := active.Add(1)
		for {
			prev := maxActive.Load()
			if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
				break
			}
		}
		if cur == int32(workers) {
			close(allRunning)
		}
		select {
		case <-allRunning:
		case <-time.After(time.Second):
		}
		active.Add(-1)
		processed.Add(1)
		return nil
	})

	require.Equal(t, int32(workers), processed.Load())
	require.GreaterOrEqual(t, maxActive.Load(), int32(2))
}

type blockingCancelChannel struct {
	blockCh     chan struct{}
	closeCalled atomic.Bool
}

func (b *blockingCancelChannel) Cancel(string, bool) error {
	<-b.blockCh
	return nil
}

func (b *blockingCancelChannel) Close() error {
	b.closeCalled.Store(true)
	return nil
}

// TestDrainRespectsContextWhenCancelBlocks 验证底层 Cancel 阻塞时 Drain 受 ctx 超时控制，且不会导致后续 Close 死锁。
func TestDrainRespectsContextWhenCancelBlocks(t *testing.T) {
	blockCh := make(chan struct{})
	defer close(blockCh)

	ch := &blockingCancelChannel{blockCh: blockCh}
	sub := &subscription{
		channel:      ch,
		consumerTag:  "consumer-1",
		done:         make(chan struct{}),
		stopCh:       make(chan struct{}),
		closeTimeout: 20 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := sub.Drain(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, ch.closeCalled.Load(), "timed out Drain should close stuck channel to unblock Cancel")

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- sub.Close()
	}()

	select {
	case closeErr := <-closeDone:
		require.Error(t, closeErr)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("sub.Close deadlocked after Drain timeout")
	}
}

// TestExchangeTypeNormalizationAndValidation 验证 fanout、direct 与非法 ExchangeType 的校验行为。
func TestExchangeTypeNormalizationAndValidation(t *testing.T) {
	_, err := normalizeConfig(Config{
		URL:          "amqp://guest:guest@127.0.0.1:5672/",
		Exchange:     "test.events",
		ExchangeType: "invalid-type",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported exchange type")

	_, err = normalizeConfig(Config{
		URL:          "amqp://guest:guest@127.0.0.1:5672/",
		Exchange:     "test.events",
		ExchangeType: "headers",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported exchange type")

	fanoutKey, err := normalizePublishRoutingKeyForExchange("", "fanout")
	require.NoError(t, err)
	require.Empty(t, fanoutKey)

	fanoutSub, err := normalizeSubscribeOptionsForExchange(SubscribeOptions{
		Queue: "fanout-queue",
	}, "fanout")
	require.NoError(t, err)
	require.Equal(t, []string{""}, fanoutSub.BindingKeys)

	directKey, err := normalizePublishRoutingKeyForExchange("order..direct*key", "direct")
	require.NoError(t, err)
	require.Equal(t, "order..direct*key", directKey)

	directSub, err := normalizeSubscribeOptionsForExchange(SubscribeOptions{
		Queue:       "direct-queue",
		BindingKeys: []string{"order..direct*key"},
	}, "direct")
	require.NoError(t, err)
	require.Equal(t, []string{"order..direct*key"}, directSub.BindingKeys)
}

// TestHandleDeliveryRecoversHandlerPanic 验证 handler panic 会被捕获并按失败路径转发，而不是拖垮进程。
func TestHandleDeliveryRecoversHandlerPanic(t *testing.T) {
	publisher := &stubPublisher{}
	client := &Client{exchange: "test.events", publisher: publisher}
	acker := &spyAcknowledger{}

	err := client.handleDelivery(context.Background(), SubscribeOptions{
		BindingKeys:   []string{"test.order.created"},
		Queue:         "example-service",
		MaxDeliveries: 3,
	}, amqp.Delivery{
		Acknowledger: acker,
		DeliveryTag:  11,
		RoutingKey:   "test.order.created",
		MessageId:    "msg-panic",
		Body:         []byte("poison"),
	}, func(context.Context, Message) error {
		panic("poison message")
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "poison message")
	require.Len(t, publisher.published, 1)
	require.Equal(t, "test.events.retry", publisher.published[0].exchange)
	require.Equal(t, "example-service", publisher.published[0].routingKey)
	require.Equal(t, uint64(11), acker.ackTag)
	require.False(t, acker.nackCalled)
}
