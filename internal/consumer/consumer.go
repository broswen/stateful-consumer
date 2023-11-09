package consumer

import (
	"context"
	"encoding/json"
	log2 "log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type Payload struct {
	ID    string `json:"id"`
	Value int64  `json:"value"`
}

func NewConsumer(redis *redis.Client, id, group, topics, brokers string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.ClientID = id
	version, err := sarama.ParseKafkaVersion("3.1.0")
	if err != nil {
		return nil, err
	}
	config.Version = version
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}

	sarama.Logger = log2.New(os.Stdout, "[sarama] ", log2.LstdFlags)

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		return nil, err
	}

	c := &Consumer{
		id:       id,
		group:    group,
		topics:   topics,
		brokers:  brokers,
		client:   client,
		redis:    redis,
		counters: map[string]int64{},
		seen:     map[string]int32{},
		ready:    make(chan interface{}),
	}
	return c, nil
}

type Consumer struct {
	id      string
	group   string
	topics  string
	brokers string
	client  sarama.ConsumerGroup
	cancel  context.CancelFunc
	redis   *redis.Client
	// counters is the in memory state for each counter
	counters map[string]int64
	ready    chan interface{}
	// seen is the last partition we saw a specific counter at, used to clean up in memory state after a rebalance
	seen map[string]int32
}

func (c *Consumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

func (c *Consumer) Consume() error {
	// wait for ready
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	for {
		if err := c.client.Consume(ctx, strings.Split(c.topics, ","), c); err != nil {
			log.Panic().Err(err)
		}
		if err := ctx.Err(); err != nil {
			log.Error().Err(err).Msg("")
			return err
		}
		c.ready = make(chan interface{})
	}
}

// CleanOtherPartitions removes all in memory state for counters that were not last seen in the provided partition set
//
//	this is used to clear in memory state after a rebalance with new partition assignment
func (c *Consumer) CleanOtherPartitions(parts map[int32]bool) {
	before := len(c.counters)
	log.Info().Msg("cleaning partitions")
	for k, v := range c.seen {
		if _, ok := parts[v]; !ok {
			log.Info().Str("id", k).Msg("cleaned")
			delete(c.counters, k)
		}
	}
	c.seen = make(map[string]int32)
	log.Info().Int("count", before-len(c.counters)).Msg("cleaned partitions")
}

// FlushCounters attempts to write all in memory counter states to redis
func (c *Consumer) FlushCounters() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	log.Info().Int("counter", len(c.counters)).Msg("flushing counters")
	for k, v := range c.counters {
		if err := c.redis.Set(ctx, k, v, 0).Err(); err != nil {
			log.Warn().Str("id", k).Int64("value", v).Msg("unable to set counter value in redis")
		}
	}
	log.Info().Int("counter", len(c.counters)).Msg("flushed counters")
}

func (c *Consumer) Handle(message *sarama.ConsumerMessage) error {
	var payload Payload
	if err := json.Unmarshal(message.Value, &payload); err != nil {
		log.Err(err).Msg("unable to unmarshal payload")
		return nil
	}

	c.seen[payload.ID] = message.Partition

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// load from in memory
	current, ok := c.counters[payload.ID]
	if !ok {
		log.Info().Str("id", payload.ID).Msg("counter doesn't exist in memory, looking up in redis")
		// load from redis if doesn't exist in memory
		value, err := c.redis.Get(ctx, payload.ID).Int64()
		if err != nil {
			log.Warn().Str("id", payload.ID).Err(err).Msg("unable to get value from redis")
		}
		// defaults to 0 if failed to get from redis
		current = value
	}
	c.counters[payload.ID] = current + payload.Value

	if err := c.redis.Set(ctx, payload.ID, c.counters[payload.ID], 0).Err(); err != nil {
		log.Warn().Str("id", payload.ID).Int64("value", c.counters[payload.ID]).Msg("unable to set counter value in redis")
	}
	return nil
}

func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	topic := strings.Split(c.topics, ",")[0]
	log.Info().Ints32(topic, session.Claims()[topic]).Msg("claimed partitions")
	parts := make(map[int32]bool)
	for _, part := range session.Claims()[topic] {
		parts[part] = true
	}
	c.CleanOtherPartitions(parts)
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	c.FlushCounters()
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			err := c.Handle(message)
			if err != nil {
				log.Error().Err(err)
				continue
			}
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
