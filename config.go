package mq

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

const (
	// DefaultPublishTimeout 表示默认等待 publisher confirm 的超时时间。
	DefaultPublishTimeout = 5 * time.Second

	// DefaultConnectTimeout 表示默认建立 RabbitMQ 连接的超时时间。
	DefaultConnectTimeout = 5 * time.Second

	// DefaultCloseTimeout 表示默认关闭订阅和连接的等待时间。
	DefaultCloseTimeout = 5 * time.Second

	// DefaultHeartbeat 表示默认 AMQP 心跳间隔。
	DefaultHeartbeat = 10 * time.Second

	// DefaultExchangeType 表示默认 exchange 类型。
	DefaultExchangeType = "topic"

	// defaultReconnectInitial 表示连接恢复的初始退避时间。
	defaultReconnectInitial = 100 * time.Millisecond

	// defaultReconnectMax 表示连接恢复的最大退避时间。
	defaultReconnectMax = 30 * time.Second
)

// Logger 定义 mq 包需要的最小日志能力。
// 外部可传入任意满足该接口的日志实现；为空时使用 slog 默认实现。
type Logger interface {
	// Infof 记录普通运行信息。
	Infof(format string, args ...any)

	// Warnf 记录可恢复但需要关注的异常分支。
	Warnf(format string, args ...any)

	// Errorf 记录真实失败路径。
	Errorf(format string, args ...any)
}

// Config 表示 RabbitMQ 客户端配置。
type Config struct {
	// URL 表示 RabbitMQ AMQP 连接地址。
	URL string

	// Name 表示客户端连接名称。
	Name string

	// Exchange 表示当前客户端固定使用的业务 exchange。
	Exchange string

	// ExchangeType 表示 exchange 类型；为空默认 topic，支持 "topic"、"direct"、"fanout"。
	// 若 Broker 上已存在同名 exchange，此处类型必须与其一致，否则初始化会返回 PRECONDITION_FAILED。
	ExchangeType string

	// ConnectTimeout 表示建立连接的超时时间。
	ConnectTimeout time.Duration

	// PublishTimeout 表示等待 publisher confirm 的超时时间。
	PublishTimeout time.Duration

	// CloseTimeout 表示关闭订阅和连接的默认等待时间。
	CloseTimeout time.Duration

	// Logger 表示模块日志记录器；为空时使用默认 slog 适配实现。
	Logger Logger
}

// slogLogger 将 slog.Logger 适配为 mq.Logger。
type slogLogger struct {
	log *slog.Logger
}

func (l *slogLogger) Infof(format string, args ...any) {
	if l == nil || l.log == nil || !l.log.Enabled(context.Background(), slog.LevelInfo) {
		return
	}
	l.log.Info(fmt.Sprintf(format, args...))
}

func (l *slogLogger) Warnf(format string, args ...any) {
	if l == nil || l.log == nil || !l.log.Enabled(context.Background(), slog.LevelWarn) {
		return
	}
	l.log.Warn(fmt.Sprintf(format, args...))
}

func (l *slogLogger) Errorf(format string, args ...any) {
	if l == nil || l.log == nil || !l.log.Enabled(context.Background(), slog.LevelError) {
		return
	}
	l.log.Error(fmt.Sprintf(format, args...))
}

// normalizeConfig 校验客户端配置，并补齐连接、发布和关闭相关默认值。
func normalizeConfig(cfg Config) (Config, error) {
	// 1. 清理字符串首尾空白并校验必填连接地址与主交换机名称。
	cfg.URL = strings.TrimSpace(cfg.URL)
	cfg.Name = strings.TrimSpace(cfg.Name)
	cfg.Exchange = strings.TrimSpace(cfg.Exchange)

	if cfg.URL == "" {
		return Config{}, fmt.Errorf("create rabbitmq client failed: url is required")
	}
	if cfg.Exchange == "" {
		return Config{}, fmt.Errorf("create rabbitmq client failed: exchange is required")
	}

	// 2. 校验并规范化交换机类型（默认 topic）。
	exchangeType, err := normalizeExchangeType(cfg.ExchangeType)
	if err != nil {
		return Config{}, err
	}
	cfg.ExchangeType = exchangeType

	// 3. 补齐建连、发布确认、关闭等待超时及默认日志器。
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = DefaultConnectTimeout
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = DefaultPublishTimeout
	}
	if cfg.CloseTimeout <= 0 {
		cfg.CloseTimeout = DefaultCloseTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = defaultLogger()
	}

	return cfg, nil
}

// normalizeExchangeType 校验并归一化 RabbitMQ 交换机类型（支持 topic/direct/fanout）。
func normalizeExchangeType(exchangeType string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(exchangeType))
	if normalized == "" {
		return DefaultExchangeType, nil
	}
	switch normalized {
	case "topic", "direct", "fanout":
		return normalized, nil
	default:
		return "", fmt.Errorf("create rabbitmq client failed: unsupported exchange type %q", exchangeType)
	}
}

// defaultLogger 返回 mq 默认日志实现。
func defaultLogger() Logger {
	return &slogLogger{log: slog.Default()}
}
