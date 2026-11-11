package mq

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type testStringer int

func (testStringer) String() string { return "sv" }

// --- fake AMQP transport (thread-safe, race-clean) ---

type fakePublishConfirm struct {
	acked bool
	err   error
	delay chan struct{}
}

func (f *fakePublishConfirm) WaitContext(ctx context.Context) (bool, error) {
	if f.delay != nil {
		select {
		case <-f.delay:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if f.err != nil {
		return false, f.err
	}
	return f.acked, nil
}

type fakeAMQPChannel struct {
	mu sync.Mutex

	closed   bool
	parent   *fakeAMQPConnection
	returnCh chan amqp.Return
	closeCh  chan *amqp.Error

	confirmErr     error
	publishErr           error
	publishConfirm       publishConfirm
	publishConfirmIsNil  bool

	exchangeDeclareErr map[string]error
	queueDeclareErr    map[string]error
	queueBindErr       map[string]error
	qosErr             error
	consumeErr         error
	cancelErr          error

	exchanges []string
	queues    []fakeQueueDeclare
	binds     []fakeQueueBind

	deliveries chan amqp.Delivery
}

type fakeQueueDeclare struct {
	name string
	args amqp.Table
}

type fakeQueueBind struct {
	queue, key, exchange string
}

func (f *fakeAMQPChannel) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeAMQPChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return amqp.ErrClosed
	}
	f.closed = true
	return nil
}

func (f *fakeAMQPChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCh = c
	return c
}

func (f *fakeAMQPChannel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.returnCh = c
	return c
}

func (f *fakeAMQPChannel) Confirm(_ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.confirmErr != nil {
		return f.confirmErr
	}
	return nil
}

func (f *fakeAMQPChannel) ExchangeDeclare(name, kind string, _, _, _, _ bool, _ amqp.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.exchangeDeclareErr[name]; err != nil {
		return err
	}
	f.exchanges = append(f.exchanges, name+":"+kind)
	return nil
}

func (f *fakeAMQPChannel) QueueDeclare(name string, _, _, _, _ bool, args amqp.Table) (amqp.Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.queueDeclareErr[name]; err != nil {
		return amqp.Queue{}, err
	}
	f.queues = append(f.queues, fakeQueueDeclare{name: name, args: args})
	return amqp.Queue{Name: name}, nil
}

func (f *fakeAMQPChannel) QueueBind(queue, key, exchange string, _ bool, _ amqp.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	bindKey := queue + "|" + key + "|" + exchange
	if err := f.queueBindErr[bindKey]; err != nil {
		return err
	}
	f.binds = append(f.binds, fakeQueueBind{queue: queue, key: key, exchange: exchange})
	return nil
}

func (f *fakeAMQPChannel) Qos(_, _ int, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.qosErr
}

func (f *fakeAMQPChannel) Consume(_, _ string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	if f.deliveries == nil {
		f.deliveries = make(chan amqp.Delivery)
	}
	return f.deliveries, nil
}

func (f *fakeAMQPChannel) Cancel(_ string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancelErr
}

func (f *fakeAMQPChannel) PublishDeferred(_ context.Context, _, _ string, _, _ bool, _ amqp.Publishing) (publishConfirm, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	if f.publishConfirmIsNil {
		return nil, nil
	}
	if f.publishConfirm != nil {
		return f.publishConfirm, nil
	}
	return &fakePublishConfirm{acked: true}, nil
}

func (f *fakeAMQPChannel) pushReturn(ret amqp.Return) {
	f.mu.Lock()
	ch := f.returnCh
	f.mu.Unlock()
	if ch != nil {
		select {
		case ch <- ret:
		default:
		}
	}
}

type fakeAMQPConnection struct {
	mu sync.Mutex

	closed     bool
	channelErr error
	channels   []*fakeAMQPChannel
	notifyCh   chan *amqp.Error
	chFactory  func() *fakeAMQPChannel
}

func (f *fakeAMQPConnection) Channel() (amqpChannel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.channelErr != nil {
		return nil, f.channelErr
	}
	var ch *fakeAMQPChannel
	if f.chFactory != nil {
		ch = f.chFactory()
	} else {
		ch = &fakeAMQPChannel{parent: f}
	}
	ch.parent = f
	f.channels = append(f.channels, ch)
	return ch, nil
}

func (f *fakeAMQPConnection) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeAMQPConnection) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return amqp.ErrClosed
	}
	f.closed = true
	return nil
}

func (f *fakeAMQPConnection) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notifyCh = c
	return c
}

func (f *fakeAMQPConnection) signalClose(err *amqp.Error) {
	f.mu.Lock()
	ch := f.notifyCh
	f.mu.Unlock()
	if ch != nil {
		select {
		case ch <- err:
		default:
		}
	}
}

func newTestClientWithConn(t *testing.T, conn *fakeAMQPConnection) *Client {
	t.Helper()
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	t.Cleanup(lifeCancel)

	pubCh := &fakeAMQPChannel{parent: conn}
	publisher := newAMQPPublisher(pubCh, time.Second)
	client := &Client{
		cfg: Config{
			URL:            "amqp://fake/",
			Exchange:       "test.events",
			ExchangeType:   DefaultExchangeType,
			ConnectTimeout: time.Second,
			PublishTimeout: time.Second,
			CloseTimeout:   time.Second,
		},
		name:           "unit",
		exchange:       "test.events",
		exchangeType:   DefaultExchangeType,
		publishTimeout: time.Second,
		closeTimeout:   time.Second,
		log:            &fakeLogger{},
		conn:           conn,
		publishCh:      pubCh,
		publisher:      publisher,
		lifeCtx:        lifeCtx,
		lifeCancel:     lifeCancel,
		subs:           make(map[*subscription]struct{}),
	}
	client.installTransport(conn, pubCh, publisher, nil, true)
	return client
}

func TestClientLifecycleAndTransport(t *testing.T) {
	t.Run("new_client_dial_failure", func(t *testing.T) {
		orig := dialTransport
		t.Cleanup(func() { dialTransport = orig })
		dialTransport = func(string, amqp.Config) (amqpConnection, error) {
			return nil, errors.New("dial refused")
		}
		_, err := NewClient(Config{URL: "amqp://x/", Exchange: "ex"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "create rabbitmq client failed")
	})

	t.Run("new_client_success", func(t *testing.T) {
		orig := dialTransport
		t.Cleanup(func() { dialTransport = orig })
		conn := &fakeAMQPConnection{}
		conn.chFactory = func() *fakeAMQPChannel {
			return &fakeAMQPChannel{parent: conn}
		}
		dialTransport = func(string, amqp.Config) (amqpConnection, error) {
			return conn, nil
		}
		client, err := NewClient(Config{URL: "amqp://x/", Exchange: "test.events"})
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })
		require.True(t, client.transportReady())
		require.Equal(t, uint64(1), client.generation)
	})

	t.Run("open_publish_channel_confirm_failure", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		conn.chFactory = func() *fakeAMQPChannel {
			return &fakeAMQPChannel{parent: conn, confirmErr: errors.New("confirm off")}
		}
		_, _, err := openPublishChannel(conn, "ex", "topic", time.Second)
		require.Error(t, err)
		require.Contains(t, err.Error(), "publisher confirm")
	})

	t.Run("establish_transport_reuse_conn", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		oldPub := client.publishCh.(*fakeAMQPChannel)
		oldPub.closed = true
		client.broken = true
		require.NoError(t, client.establishTransport())
		require.True(t, client.transportReady())
	})

	t.Run("install_transport_life_canceled", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		client.lifeCancel()
		ch := &fakeAMQPChannel{parent: conn}
		err := client.installTransport(conn, ch, newAMQPPublisher(ch, time.Second), nil, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "client already closed")
	})

	t.Run("watch_transport_mark_broken_stale_gen", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		ch := client.publishCh.(*fakeAMQPChannel)
		client.generation = 5
		client.watchTransport(conn, ch, 3)
		conn.signalClose(amqp.ErrClosed)
		time.Sleep(20 * time.Millisecond)
		require.False(t, client.broken, "stale generation must not mark broken")
	})

	t.Run("watch_transport_mark_broken", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		gen := client.generation
		ch := client.publishCh.(*fakeAMQPChannel)
		client.watchTransport(conn, ch, gen)
		ch.closeCh <- amqp.ErrClosed
		require.Eventually(t, func() bool {
			client.reconnectMu.Lock()
			defer client.reconnectMu.Unlock()
			return client.broken
		}, time.Second, 5*time.Millisecond)
	})

	t.Run("ensure_connected_singleflight", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		client.broken = true
		client.publishCh.(*fakeAMQPChannel).closed = true
		conn.closed = true

		orig := dialTransport
		t.Cleanup(func() { dialTransport = orig })
		var dials atomic.Int32
		dialTransport = func(string, amqp.Config) (amqpConnection, error) {
			n := dials.Add(1)
			if n == 1 {
				time.Sleep(50 * time.Millisecond)
			}
			conn.mu.Lock()
			conn.closed = false
			conn.mu.Unlock()
			return conn, nil
		}

		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- client.ensureConnected(context.Background())
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		require.Equal(t, int32(1), dials.Load())
	})

	t.Run("reconnect_ctx_canceled", func(t *testing.T) {
		conn := &fakeAMQPConnection{closed: true}
		client := newTestClientWithConn(t, conn)
		client.broken = true
		client.publishCh.(*fakeAMQPChannel).closed = true
		orig := dialTransport
		t.Cleanup(func() { dialTransport = orig })
		dialTransport = func(string, amqp.Config) (amqpConnection, error) {
			return nil, errors.New("still down")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := client.reconnectWithBackoff(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "reconnect rabbitmq failed")
	})

	t.Run("reconnect_life_ctx_done", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		client.broken = true
		orig := dialTransport
		t.Cleanup(func() { dialTransport = orig })
		dialTransport = func(string, amqp.Config) (amqpConnection, error) {
			return nil, errors.New("still down")
		}
		client.lifeCancel()
		err := client.reconnectWithBackoff(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "client already closed")
	})

	t.Run("client_close_idempotent", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		require.NoError(t, client.Close())
		require.NoError(t, client.Close())
	})

	t.Run("open_channel_nil_conn", func(t *testing.T) {
		client := &Client{}
		_, err := client.openChannel()
		require.ErrorIs(t, err, amqp.ErrClosed)
	})

	t.Run("close_amqp_nil_and_err_closed", func(t *testing.T) {
		require.NoError(t, closeAMQP(nil))
		var p *fakeAMQPConnection
		require.NoError(t, closeAMQP(p))
		ch := &fakeAMQPChannel{}
		_ = ch.Close()
		require.NoError(t, closeAMQP(ch))
	})
}

func TestAMQPPublisherAndPublishMessage(t *testing.T) {
	t.Run("publish_success_mandatory_return", func(t *testing.T) {
		waitConfirm := make(chan struct{})
		ch := &fakeAMQPChannel{
			publishConfirm: &fakePublishConfirm{acked: true, delay: waitConfirm},
		}
		pub := newAMQPPublisher(ch, time.Second)
		errCh := make(chan error, 1)
		go func() {
			errCh <- pub.Publish(context.Background(), "ex", "k", amqp.Publishing{Body: []byte("x")}, true)
		}()
		time.Sleep(20 * time.Millisecond)
		ch.pushReturn(amqp.Return{
			ReplyCode: 312,
			ReplyText: "NO_ROUTE",
			Headers:   amqp.Table{publishSeqHeader: "1"},
		})
		close(waitConfirm)
		err := <-errCh
		require.Error(t, err)
		require.Contains(t, err.Error(), "message returned by broker")
	})

	t.Run("publish_closed_channel", func(t *testing.T) {
		ch := &fakeAMQPChannel{closed: true}
		pub := newAMQPPublisher(ch, time.Second)
		err := pub.Publish(context.Background(), "ex", "k", amqp.Publishing{Body: []byte("x")}, false)
		var outcome *publishOutcomeError
		require.True(t, errors.As(err, &outcome))
		require.False(t, outcome.submitted)
	})

	t.Run("publish_confirm_nil", func(t *testing.T) {
		ch := &fakeAMQPChannel{publishConfirmIsNil: true}
		pub := newAMQPPublisher(ch, time.Second)
		err := pub.Publish(context.Background(), "ex", "k", amqp.Publishing{Body: []byte("x")}, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "publisher confirm is not enabled")
	})

	t.Run("publish_wait_timeout_submitted", func(t *testing.T) {
		delay := make(chan struct{})
		ch := &fakeAMQPChannel{
			publishConfirm: &fakePublishConfirm{delay: delay},
		}
		pub := newAMQPPublisher(ch, 30*time.Millisecond)
		err := pub.Publish(context.Background(), "ex", "k", amqp.Publishing{Body: []byte("x")}, false)
		var outcome *publishOutcomeError
		require.True(t, errors.As(err, &outcome))
		require.True(t, outcome.submitted)
		require.Contains(t, err.Error(), "delivery outcome unknown")
	})

	t.Run("publish_nack", func(t *testing.T) {
		ch := &fakeAMQPChannel{
			publishConfirm: &fakePublishConfirm{acked: false},
		}
		pub := newAMQPPublisher(ch, time.Second)
		err := pub.Publish(context.Background(), "ex", "k", amqp.Publishing{Body: []byte("x")}, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "nacked by broker")
	})

	t.Run("publish_outcome_error_nil_receiver", func(t *testing.T) {
		var e *publishOutcomeError
		require.Equal(t, "publish outcome error", e.Error())
		require.Nil(t, e.Unwrap())
	})

	t.Run("client_publish_routing_validation", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		err := client.Publish(context.Background(), "bad.*", []byte("x"))
		require.Error(t, err)
	})

	t.Run("client_publish_reconnect_retry", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		pubCh := client.publishCh.(*fakeAMQPChannel)
		pubCh.publishErr = amqp.ErrClosed
		client.broken = true
		orig := dialTransport
		t.Cleanup(func() { dialTransport = orig })
		dialTransport = func(string, amqp.Config) (amqpConnection, error) { return conn, nil }
		err := client.Publish(context.Background(), "test.order.created", []byte("x"))
		require.NoError(t, err)
	})

	t.Run("fanout_empty_routing_key", func(t *testing.T) {
		key, err := normalizePublishRoutingKeyForExchange("  ", "fanout")
		require.NoError(t, err)
		require.Empty(t, key)
	})
}

func TestTopologyDeclarationAndEnsure(t *testing.T) {
	t.Run("ttl_exceeds_max_int32", func(t *testing.T) {
		_, err := ttlMilliseconds(time.Duration(math.MaxInt32+1) * time.Millisecond)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds rabbitmq ttl limit")
	})

	t.Run("ensure_topology_success", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		ch := &fakeAMQPChannel{parent: conn}
		conn.chFactory = func() *fakeAMQPChannel { return ch }

		opts := SubscribeOptions{
			Queue:       "q1",
			BindingKeys: []string{"k.*"},
			RetryDelay:  2 * time.Second,
		}
		require.NoError(t, client.EnsureTopology(context.Background(), opts))
		require.Contains(t, strings.Join(ch.exchanges, ","), "test.events:topic")
		require.Contains(t, strings.Join(ch.exchanges, ","), ".retry:direct")
		require.Contains(t, strings.Join(ch.exchanges, ","), ".dlx:direct")

		var retryQ *fakeQueueDeclare
		for i := range ch.queues {
			if strings.Contains(ch.queues[i].name, ".retry.") {
				retryQ = &ch.queues[i]
				break
			}
		}
		require.NotNil(t, retryQ)
		require.Equal(t, "", retryQ.args["x-dead-letter-exchange"])
		require.Equal(t, "q1", retryQ.args["x-dead-letter-routing-key"])
	})

	t.Run("declare_topology_ctx_canceled", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		ch := &fakeAMQPChannel{parent: conn}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := client.declareTopology(ctx, ch, SubscribeOptions{Queue: "q", BindingKeys: []string{"k"}})
		require.Error(t, err)
		require.Contains(t, err.Error(), "context canceled")
	})

	t.Run("declare_topology_exchange_failures", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		base := SubscribeOptions{Queue: "q", BindingKeys: []string{"k"}, RetryDelay: time.Second}

		cases := []struct {
			name string
			fail string
			want string
		}{
			{"main", client.exchange, "declare main exchange"},
			{"retry", retryExchangeName(client.exchange), "declare retry exchange"},
			{"dlx", deadLetterExchangeName(client.exchange), "declare dead letter exchange"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ch := &fakeAMQPChannel{
					parent:             conn,
					exchangeDeclareErr: map[string]error{tc.fail: errors.New("boom")},
				}
				err := client.declareTopology(context.Background(), ch, base)
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.want)
			})
		}
	})

	t.Run("declare_topology_queue_bind_failures", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		base := SubscribeOptions{Queue: "q", BindingKeys: []string{"k"}, RetryDelay: time.Second}
		retryQ := retryQueueName("q", time.Second)
		dlq := dlqName("q")

		ch := &fakeAMQPChannel{parent: conn, queueDeclareErr: map[string]error{"q": errors.New("qfail")}}
		require.ErrorContains(t, client.declareTopology(context.Background(), ch, base), "declare queue")

		ch = &fakeAMQPChannel{parent: conn, queueDeclareErr: map[string]error{retryQ: errors.New("rfail")}}
		require.ErrorContains(t, client.declareTopology(context.Background(), ch, base), "declare retry queue")

		ch = &fakeAMQPChannel{parent: conn, queueDeclareErr: map[string]error{dlq: errors.New("dfail")}}
		require.ErrorContains(t, client.declareTopology(context.Background(), ch, base), "declare dlq")

		ch = &fakeAMQPChannel{parent: conn, queueBindErr: map[string]error{"q|k|test.events": errors.New("bfail")}}
		require.ErrorContains(t, client.declareTopology(context.Background(), ch, base), "bind queue")

		ch = &fakeAMQPChannel{parent: conn, queueBindErr: map[string]error{retryQ + "|q|" + retryExchangeName(client.exchange): errors.New("rb")}}
		require.ErrorContains(t, client.declareTopology(context.Background(), ch, base), "bind retry queue")

		ch = &fakeAMQPChannel{parent: conn, queueBindErr: map[string]error{dlq + "|q|" + deadLetterExchangeName(client.exchange): errors.New("db")}}
		require.ErrorContains(t, client.declareTopology(context.Background(), ch, base), "bind dlq")
	})

	t.Run("ensure_topology_open_channel_fail", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		conn.channelErr = errors.New("channel refused")
		err := client.EnsureTopology(context.Background(), SubscribeOptions{
			Queue: "q", BindingKeys: []string{"k"}, RetryDelay: time.Second,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "open channel")
	})
}

func TestSubscribeLifecycleAndRecovery(t *testing.T) {
	t.Run("subscribe_nil_handler", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		_, err := client.Subscribe(context.Background(), SubscribeOptions{Queue: "q", BindingKeys: []string{"k"}}, nil)
		require.Error(t, err)
	})

	t.Run("start_consume_failures", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		sub := &subscription{opts: SubscribeOptions{Queue: "q", BindingKeys: []string{"k"}, RetryDelay: time.Second, Prefetch: 1}}

		ch := &fakeAMQPChannel{parent: conn, qosErr: errors.New("qos")}
		conn.chFactory = func() *fakeAMQPChannel { return ch }
		_, _, err := client.startConsume(context.Background(), sub)
		require.Error(t, err)
		require.Contains(t, err.Error(), "qos")

		ch.qosErr = nil
		ch.consumeErr = errors.New("consume fail")
		_, _, err = client.startConsume(context.Background(), sub)
		require.Error(t, err)
		require.Contains(t, err.Error(), "consume")
	})

	t.Run("locked_acknowledger_reject", func(t *testing.T) {
		var mu sync.Mutex
		inner := &spyAcknowledger{}
		la := &lockedAcknowledger{mu: &mu, next: inner}
		require.NoError(t, la.Reject(1, false))
	})

	t.Run("consume_error_unwrap", func(t *testing.T) {
		inner := errors.New("inner")
		ce := &consumeError{err: inner, terminal: true}
		require.Equal(t, "inner", ce.Error())
		require.Same(t, inner, ce.Unwrap())
		var nilCE *consumeError
		require.Equal(t, "consume error", nilCE.Error())
	})

	t.Run("prepare_recover_stop_immediate", func(t *testing.T) {
		conn := &fakeAMQPConnection{}
		client := newTestClientWithConn(t, conn)
		sub := &subscription{
			client:  client,
			opts:    SubscribeOptions{Queue: "q", BindingKeys: []string{"k"}, RetryDelay: time.Second},
			stopCh:  make(chan struct{}),
			done:    make(chan struct{}),
			parentCtx: context.Background(),
		}
		conn.chFactory = func() *fakeAMQPChannel {
			return &fakeAMQPChannel{parent: conn, consumeErr: errors.New("consume fail")}
		}
		close(sub.stopCh)
		_, cancel, _, action := client.prepareSubscriptionLoop(sub, nil)
		if cancel != nil {
			cancel()
		}
		require.Equal(t, subscriptionLoopExit, action)
	})
}

func TestConfigMessageAndOptionsEdgeCases(t *testing.T) {
	t.Run("slog_logger_levels", func(t *testing.T) {
		var buf strings.Builder
		h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})
		l := &slogLogger{log: slog.New(h)}
		l.Infof("skip %s", "info")
		l.Warnf("skip %s", "warn")
		l.Errorf("err %s", "x")
		require.Contains(t, buf.String(), "err x")

		var nilLog *slogLogger
		require.NotPanics(t, func() { nilLog.Infof("x") })
		nilInner := &slogLogger{}
		require.NotPanics(t, func() { nilInner.Warnf("x") })
	})

	t.Run("message_helpers", func(t *testing.T) {
		require.Equal(t, "rk", originalRoutingKeyFromDelivery(amqp.Delivery{
			Headers: amqp.Table{originalRoutingKeyHeader: []byte("  rk  ")},
		}))
		require.Nil(t, headersToTable(nil))
		require.NotNil(t, headersToTable(map[string]string{"a": "b"}))

		h := tableToHeaders(amqp.Table{"s": testStringer(1), "n": 42})
		require.Equal(t, "sv", h["s"])
		require.Equal(t, "42", h["n"])

		require.Equal(t, 2, retryCountFromHeaders(amqp.Table{retryCountHeader: int8(2)}))
		require.Equal(t, 3, retryCountFromHeaders(amqp.Table{retryCountHeader: uint32(3)}))

		seq, ok := publishSeqFromHeaders(amqp.Table{publishSeqHeader: uint64(7)})
		require.True(t, ok)
		require.Equal(t, uint64(7), seq)
		_, ok = publishSeqFromHeaders(amqp.Table{publishSeqHeader: "bad"})
		require.False(t, ok)
	})

	t.Run("with_headers_empty", func(t *testing.T) {
		opts := PublishOptions{}
		WithHeaders(map[string]string{})(&opts)
		require.Nil(t, opts.Headers)
		opts.Headers = map[string]string{"a": "1"}
		WithHeaders(map[string]string{"b": "2"})(&opts)
		require.Equal(t, "1", opts.Headers["a"])
		require.Equal(t, "2", opts.Headers["b"])
	})

	t.Run("fanout_binding_trim", func(t *testing.T) {
		got, err := normalizeSubscribeOptionsForExchange(SubscribeOptions{
			Queue: "q", BindingKeys: []string{" ", "x"}, RetryDelay: time.Second,
		}, "fanout")
		require.NoError(t, err)
		require.Equal(t, []string{"", "x"}, got.BindingKeys)
	})

	t.Run("direct_empty_binding", func(t *testing.T) {
		_, err := normalizeSubscribeOptionsForExchange(SubscribeOptions{
			Queue: "q", BindingKeys: []string{" "},
		}, "direct")
		require.Error(t, err)
	})
}

func TestConsumeLoopConcurrentExit(t *testing.T) {
	client := &Client{exchange: "ex", log: &fakeLogger{}}
	sub := &subscription{done: make(chan struct{})}
	deliveries := make(chan amqp.Delivery)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client.consumeLoopConcurrent(ctx, sub, SubscribeOptions{Concurrency: 2}, deliveries, func(context.Context, Message) error {
		return nil
	})
}

func TestPublishSeqInternalExhaustive(t *testing.T) {
	tests := []struct {
		table amqp.Table
		ok    bool
	}{
		{nil, false},
		{amqp.Table{publishSeqHeader: int64(5)}, true},
		{amqp.Table{publishSeqHeader: int(9)}, true},
		{amqp.Table{publishSeqHeader: []byte("11")}, true},
		{amqp.Table{publishSeqHeader: int64(0)}, false},
	}
	for _, tt := range tests {
		_, ok := publishSeqFromHeaders(tt.table)
		require.Equal(t, tt.ok, ok)
	}
}

func TestReconnectBackoffEventuallySucceeds(t *testing.T) {
	conn := &fakeAMQPConnection{}
	client := newTestClientWithConn(t, conn)
	client.broken = true
	orig := dialTransport
	t.Cleanup(func() { dialTransport = orig })
	var calls atomic.Int32
	dialTransport = func(string, amqp.Config) (amqpConnection, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("transient")
		}
		return conn, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, client.reconnectWithBackoff(ctx))
}

// noopWriter for slog discard tests
type noopWriter struct{}

func (noopWriter) Write([]byte) (int, error) { return 0, io.EOF }

func TestSlogLoggerInfoWhenEnabled(t *testing.T) {
	w := &noopWriter{}
	h := slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug})
	l := &slogLogger{log: slog.New(h)}
	require.NotPanics(t, func() { l.Infof("hello") })
}
