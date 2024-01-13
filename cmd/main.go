package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/broswen/stateful-consumer/internal/consumer"
)

func main() {

	brokers := os.Getenv("BROKERS")
	if brokers == "" {
		log.Fatal().Msgf("kafka brokers are empty")
	}
	topic := os.Getenv("TOPIC")
	if topic == "" {
		log.Fatal().Msgf("request topic is empty")
	}
	redisHost := os.Getenv("REDIS")
	if redisHost == "" {
		log.Fatal().Msgf("redis is empty")
	}

	// initialize redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)

	// TODO pass in ctx here for internal use as well
	c, err := consumer.NewConsumer(rdb, "stateful-consumer", "stateful-consumer", topic, brokers)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to create consumer")
	}

	eg.Go(func() error {
		if err := c.Consume(ctx); err != nil {
			log.Err(err).Msg("error consuming from kafka")
			return err
		}
		log.Info().Msg("consumer completed")
		return nil
	})

	if err = eg.Wait(); err != nil {
		log.Error().Err(err).Msg("caught error")
	}
}
