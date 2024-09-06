package config

import (
	"flag"
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	DEFAULT_DEBUG = false

	DEFAULT_IDLE_TIMEOUT_SECONDS = 5

	DEFAULT_CONSUMER_MAX_OUTSTANDING_MESSAGES = 100000

	DEFAULT_PRODUCER_DATABASE               = "default"
	DEFAULT_PRODUCER_USER                   = "default"
	DEFAULT_PRODUCER_PORT                   = 9440
	DEFAULT_PRODUCER_WRITE_INTERVAL_SECONDS = 3
)

type Config struct {
	Debug bool `koanf:"debug"`

	IdleTimeoutSeconds uint8 `koanf:"idle_timeout_seconds"`

	Consumer ConsumerConfig `koanf:"consumer"`
	Producer ProducerConfig `koanf:"producer"`
}

type ConsumerConfig struct {
	ProjectId      string `koanf:"project_id"`
	TopicId        string `koanf:"topic_id"`
	SubscriptionId string `koanf:"subscription_id"`

	MaxOutstandingMessages int `koanf:"max_outstanding_messages"`
}

type ProducerConfig struct {
	Host                 string `koanf:"host"`
	Database             string `koanf:"database"`
	Port                 int    `koanf:"port"`
	User                 string `koanf:"user"`
	Password             string `koanf:"password"`
	Table                string `koanf:"table"`
	WriteIntervalSeconds uint8  `koanf:"write_interval_seconds"`
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
	err := k.Load(confmap.Provider(map[string]interface{}{
		"debug": DEFAULT_DEBUG,

		"idle_timeout_seconds": DEFAULT_IDLE_TIMEOUT_SECONDS,

		"consumer.max_outstanding_messages": DEFAULT_CONSUMER_MAX_OUTSTANDING_MESSAGES,

		"producer.database":               DEFAULT_PRODUCER_DATABASE,
		"producer.port":                   DEFAULT_PRODUCER_PORT,
		"producer.user":                   DEFAULT_PRODUCER_USER,
		"producer.write_interval_seconds": DEFAULT_PRODUCER_WRITE_INTERVAL_SECONDS,
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
