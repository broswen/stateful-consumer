package main

import (
	"os"
	"os/signal"
	"syscall"

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
	topics := os.Getenv("TOPICS")
	if topics == "" {
		log.Fatal().Msgf("request topics list is empty")
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

	eg := errgroup.Group{}

	c, err := consumer.NewConsumer(rdb, "stateful-consumer", "stateful-consumer", topics, brokers)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to create consumer")
	}

	eg.Go(func() error {
		if err := c.Consume(); err != nil {
			log.Err(err).Msg("error starting kafka consumer")
			return err
		}
		return nil
	})
	defer c.Close()

	eg.Go(func() error {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
		s := <-sigs
		log.Debug().Str("signal", s.String()).Msg("received signal")
		if err := c.Close(); err != nil {
			log.Error().Err(err).Msg("error closing kafka consumer")
			return err
		}
		return nil
	})

	if err = eg.Wait(); err != nil {
		log.Error().Err(err).Msg("caught error")
	}
}
