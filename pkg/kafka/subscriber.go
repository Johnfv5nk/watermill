package kafka

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type Subscriber struct {
	brokers      []string
	saramaConfig *sarama.Config
	config       SubscriberConfig

	logger watermill.LoggerAdapter

	closing chan struct{}
	closed  bool

	clients        []sarama.Client
	consumerGroups []sarama.ConsumerGroup

	closedWg  sync.WaitGroup
	closingWg sync.WaitGroup
}

func NewSubscriber(
	config SubscriberConfig,
	logger watermill.LoggerAdapter,
) (*Subscriber, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	saramaConfig := config.OverwriteSaramaConfig
	if saramaConfig == nil {
		saramaConfig = DefaultSaramaSubscriberConfig()
	}

	return &Subscriber{
		brokers:      config.Brokers,
		saramaConfig: saramaConfig,
		config:       config,
		logger:       logger,
		closing:      make(chan struct{}),
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.closingWg.Add(1)

	err := s.config.OverwriteSaramaConfigIfNeeded(s.saramaConfig)
	if err != nil {
		return nil, err
	}

	if err := s.saramaConfig.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid sarama config")
	}

	client, err := sarama.NewClient(s.brokers, s.saramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create new sarama client")
	}

	group := s.config.ConsumerGroup
	if group == "" {
		group = watermill.NewUUID()
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		client.Close()
		return nil, errors.Wrap(err, "cannot create new sarama consumer group")
	}

	s.consumerGroups = append(s.consumerGroups, consumerGroup)
	s.clients = append(s.clients, client)

	output := make(chan *message.Message, s.config.NackResendSleep)

	go func() {
		<-ctx.Done()
		consumerGroup.Close()
		client.Close()
	}()

	go s.consume(ctx, topic, output, group, consumerGroup)

	return output, nil
}

func (s *Subscriber) consume(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	group string,
	consumerGroup sarama.ConsumerGroup,
) {
	s.closedWg.Add(1)
	defer s.closedWg.Done()

	logger := s.logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": group,
	})

	var backoff time.Duration = 100 * time.Millisecond
	maxBackoff := 10 * time.Second
	reconnecting := false

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		handler := &consumerGroupHandler{
			ready:             make(chan struct{}),
			output:            output,
			decodingSignature: s.config.Unmarshaler.Description(),
			unmarshaler:       s.config.Unmarshaler,
			logger:            logger,
			closing:           s.closing,
			isReconnecting:    reconnecting,
			onSetup: func() {
				reconnecting = false
				backoff = 100 * time.Millisecond
			},
		}

		if reconnecting {
			logger.Info("Reconnecting to Kafka broker", nil)
		}

		err := consumerGroup.Consume(ctx, []string{topic}, handler)
		if err != nil {
			if err == sarama.ErrClosedConsumerGroup {
				return
			}

			reconnecting = true
			logger.Error("sarama consume failed, retrying with backoff", err, watermill.LogFields{
				"backoff": backoff.String(),
			})

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			// Exponential backoff with jitter
			backoff = time.Duration(float64(backoff) * 1.5)
			jitter := time.Duration(rand.Float64()*0.4 - 0.2) * backoff
			backoff += jitter
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			if backoff < 100*time.Millisecond {
				backoff = 100 * time.Millisecond
			}
			continue
		}
	}
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)

	s.closingWg.Done()
	s.closingWg.Wait()

	var errs []error

	for _, consumerGroup := range s.consumerGroups {
		if err := consumerGroup.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	for _, client := range s.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	s.closedWg.Wait()

	if len(errs) > 0 {
		return errors.Errorf("errors closing subscriber: %v", errs)
	}

	return nil
}

type consumerGroupHandler struct {
	ready             chan struct{}
	output            chan *message.Message
	decodingSignature string
	unmarshaler       Unmarshaler
	logger            watermill.LoggerAdapter
	closing           chan struct{}
	isReconnecting    bool
	onSetup           func()
}

func (c *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	if c.isReconnecting {
		c.logger.Info("Successfully reconnected to Kafka broker", nil)
	}
	if c.onSetup != nil {
		c.onSetup()
	}
	close(c.ready)
	return nil
}

func (c *consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			logger := c.logger.With(watermill.LogFields{
				"kafka_partition": msg.Partition,
				"kafka_offset":    msg.Offset,
			})

			receivedMsg, err := c.unmarshaler.Unmarshal(msg)
			if err != nil {
				logger.Error("cannot unmarshal message", err, nil)
				continue
			}

			ctx, cancelCtx := context.WithCancel(session.Context())
			receivedMsg.SetContext(ctx)
			defer cancelCtx()

			c.output <- receivedMsg

			select {
			case <-receivedMsg.Acked():
				session.MarkMessage(msg, "")
			case <-receivedMsg.Nacked():
				// nack is not supported by kafka, we just don't commit
			case <-c.closing:
				return nil
			case <-session.Context().Done():
				return nil
			}

		case <-c.closing:
			return nil
		case <-session.Context().Done():
			return nil
		}
	}
}