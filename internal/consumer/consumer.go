package consumer

import (
	"context"
	"encoding/json"
	log2 "log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type Payload struct {
	ID    string `json:"id"`
	Value int64  `json:"value"`
}

func NewConsumer(redis *redis.Client, id, group, topic, brokers string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.ClientID = id
	version, err := sarama.ParseKafkaVersion("3.1.0")
	if err != nil {
		return nil, err
	}
	config.Version = version
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	instanceId := os.Getenv("INSTANCE_ID")
	if instanceId != "" {
		log.Info().Str("instance_id", instanceId).Msg("set consumer group static instance id")
		config.Consumer.Group.InstanceId = instanceId
	}
	sarama.Logger = log2.New(os.Stdout, "[sarama] ", log2.LstdFlags)

	client, err := sarama.NewClient(strings.Split(brokers, ","), config)
	if err != nil {
		log.Error().Err(err).Msg("unable to create sarama client")
		return nil, err
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		log.Error().Err(err).Msg("unable to create sarama consumer group from client")
		return nil, err
	}

	c := &Consumer{
		id:            id,
		group:         group,
		topic:         topic,
		brokers:       brokers,
		client:        client,
		consumerGroup: consumerGroup,
		redis:         redis,
		counters:      map[string]int64{},
		seen:          map[string]int32{},
	}
	return c, nil
}

type Consumer struct {
	sync.Mutex
	id            string
	group         string
	topic         string
	brokers       string
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	cancel        context.CancelFunc
	redis         *redis.Client
	// counters is the in memory state for each counter
	counters map[string]int64
	// seen is the last partition we saw a specific counter at, used to clean up in memory state after a rebalance
	seen map[string]int32
}

func (c *Consumer) PartitionInfo() (map[string][]int32, error) {
	topics, err := c.client.Topics()
	if err != nil {
		return nil, err
	}
	info := make(map[string][]int32)
	for _, topic := range topics {
		partitions, err := c.client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		info[topic] = partitions
	}
	return info, nil
}

func (c *Consumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	if err := c.consumerGroup.Close(); err != nil {
		return err
	}
	return c.client.Close()
}

func (c *Consumer) Consume(ctx context.Context) error {
	for {
		if err := c.consumerGroup.Consume(ctx, []string{c.topic}, c); err != nil {
			log.Panic().Err(err)
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
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

	c.Lock()
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
	c.Unlock()

	if err := c.redis.Set(ctx, payload.ID, c.counters[payload.ID], 0).Err(); err != nil {
		log.Warn().Str("id", payload.ID).Int64("value", c.counters[payload.ID]).Msg("unable to set counter value in redis")
	}
	return nil
}

func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	log.Info().Ints32(c.topic, session.Claims()[c.topic]).Msg("claimed partitions")
	parts := make(map[int32]bool)
	for _, part := range session.Claims()[c.topic] {
		parts[part] = true
	}
	c.CleanOtherPartitions(parts)
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
			log.Info().Msg("context done, stopping consume claim")
			return nil
		}
	}
}
