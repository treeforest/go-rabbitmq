// Package mq 基于 RabbitMQ AMQP 0-9-1 提供服务间异步消息发布与订阅能力。
// 该文件定义客户端配置、日志抽象和配置默认值。
package mq

import (
	"fmt"
	"strings"
	"time"

	"github.com/treeforest/golog/v2"
)

const (
	// DefaultPublishTimeout 表示默认等待 publisher confirm 的超时时间。
	DefaultPublishTimeout = 5 * time.Second

	// DefaultConnectTimeout 表示默认建立 RabbitMQ 连接的超时时间。
	DefaultConnectTimeout = 5 * time.Second

	// DefaultCloseTimeout 表示默认关闭订阅和连接的等待时间。
	DefaultCloseTimeout = 5 * time.Second

	// DefaultExchangeType 表示默认 exchange 类型。
	DefaultExchangeType = "topic"
)

// Logger 定义 mq 包需要的最小日志能力。
// 外部可传入任意满足该接口的日志实现；为空时使用 golog 默认实现。
type Logger interface {
	// Infof 记录普通运行信息。
	Infof(format string, args ...interface{})

	// Warnf 记录可恢复但需要关注的异常分支。
	Warnf(format string, args ...interface{})

	// Errorf 记录真实失败路径。
	Errorf(format string, args ...interface{})
}

// Config 表示 RabbitMQ 客户端配置。
type Config struct {
	// URL 表示 RabbitMQ AMQP 连接地址。
	URL string

	// Name 表示客户端连接名称。
	Name string

	// Exchange 表示当前客户端固定使用的业务 exchange。
	Exchange string

	// ExchangeType 表示 exchange 类型；为空默认 topic。
	ExchangeType string

	// ConnectTimeout 表示建立连接的超时时间。
	ConnectTimeout time.Duration

	// PublishTimeout 表示等待 publisher confirm 的超时时间。
	PublishTimeout time.Duration

	// CloseTimeout 表示关闭订阅和连接的默认等待时间。
	CloseTimeout time.Duration

	// Logger 表示模块日志记录器；为空时使用默认 golog 适配实现。
	Logger Logger
}

// normalizeConfig 校验客户端配置，并补齐连接、发布和关闭相关默认值。
func normalizeConfig(cfg Config) (Config, error) {
	cfg.URL = strings.TrimSpace(cfg.URL)
	cfg.Name = strings.TrimSpace(cfg.Name)
	cfg.Exchange = strings.TrimSpace(cfg.Exchange)
	cfg.ExchangeType = strings.TrimSpace(cfg.ExchangeType)

	if cfg.URL == "" {
		return Config{}, fmt.Errorf("create rabbitmq client failed: url is required")
	}
	if cfg.Exchange == "" {
		return Config{}, fmt.Errorf("create rabbitmq client failed: exchange is required")
	}
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = DefaultExchangeType
	}
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

// defaultLogger 返回 mq 默认日志实现。
func defaultLogger() Logger {
	return golog.NewLogger(golog.NewConfig()).Clone()
}
