package config

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	DEFAULT_DEBUG                           = false
	DEFAULT_CONSUMER_PORT                   = 9440
	DEFAULT_PRODUCER_WRITE_INTERVAL_SECONDS = 3
)

type Config struct {
	Debug bool `koanf:"debug"`

	Clickhouse ClickhouseConfig `koanf:"clickhouse"`

	Consumer ConsumerConfig `koanf:"consumer"`

	PubsubProducer     PubsubProducerConfig     `koanf:"pubsub_producer"`
	ClickhouseProducer ClickhouseProducerConfig `koanf:"clickhouse_producer"`
}

type ClickhouseConfig struct {
	Host     string `koanf:"host"`
	Port     uint32 `koanf:"port"`
	Database string `koanf:"database"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
}

type ConsumerConfig struct {
	Start time.Time `koanf:"start"`
	End   time.Time `koanf:"end"`
}

type PubsubProducerConfig struct {
	ProjectId string `koanf:"project_id"`
	TopicId   string `koanf:"topic_id"`
}

type ClickhouseProducerConfig struct {
	Table                string `koanf:"table"`
	WriteIntervalSeconds int64  `koanf:"write_interval_seconds"`
}

func MustNew() *Config {
	var c Config

	k := koanf.New(".")

	mustLoadDefaults(k)

	fileFlag := mustCheckFileFlag()
	if fileFlag != "" {
		mustLoadYamlFile(k, fileFlag)
	}

	mustLoadEnv(k)

	err := k.Unmarshal("", &c)
	if err != nil {
		panic(fmt.Errorf("error while unmarshalling config: %w", err))
	}

	mustLoadCloudRunEnv(&c)

	return &c
}

func mustLoadDefaults(k *koanf.Koanf) {
	end := time.Now().UTC().Truncate(time.Hour)
	start := end.Add(-2 * time.Duration(time.Hour))

	err := k.Load(confmap.Provider(map[string]interface{}{
		"debug": DEFAULT_DEBUG,

		"clickhouse.port": DEFAULT_CONSUMER_PORT,

		"consumer.start": start,
		"consumer.end":   end,

		"clickhouse_producer.write_interval_seconds": DEFAULT_PRODUCER_WRITE_INTERVAL_SECONDS,
	}, "."), nil)
	if err != nil {
		panic(fmt.Errorf("error while loading config defaults: %w", err))
	}
}

func mustCheckFileFlag() string {
	var fFlag = flag.String("cfile", "", "Path to the configuration YAML file")

	flag.Parse()

	return *fFlag
}

func mustLoadYamlFile(k *koanf.Koanf, name string) {
	err := k.Load(file.Provider(name), yaml.Parser())
	if err != nil {
		panic(fmt.Errorf("error while loading yaml config file: %w", err))
	}
}

func mustLoadEnv(k *koanf.Koanf) {
	err := k.Load(env.Provider("CONNECTOR_", ".", func(s string) string {
		return strings.Replace(strings.Replace(strings.ToLower(
			strings.TrimPrefix(s, "CONNECTOR_")), "_", ".", -1), "-", "_", -1)
	}), nil)

	if err != nil {
		panic(err)
	}
}

func mustLoadCloudRunEnv(c *Config) {
}
