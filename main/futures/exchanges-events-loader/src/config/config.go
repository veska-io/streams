package config

import (
	"flag"
	"fmt"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	DEFAULT_DEBUG = false

	DEFAULT_PUBSUB_IDLE_TIMEOUT_SECONDS     = 10
	DEFAULT_PUBSUB_MAX_OUTSTANDING_MESSAGES = 100000

	DEFAULT_CLICKHOUSE_DB                     = "default"
	DEFAULT_CLICKHOUSE_USER                   = "default"
	DEFAULT_CLICKHOUSE_WRITE_INTERVAL_SECONDS = 3
)

type Config struct {
	Debug bool `koanf:"CONNECTOR_DEBUG"`

	PubSubProjectId string `koanf:"CONNECTOR_PUBSUB_PROJECT_ID"`
	PubSubTopic     string `koanf:"CONNECTOR_PUBSUB_TOPIC"`
	SubscriptionId  string `koanf:"CONNECTOR_PUBSUB_SUBSCRIPTION_ID"`

	IdleTimeoutSeconds     uint8 `koanf:"CONNECTOR_PUBSUB_IDLE_TIMEOUT_SECONDS"`
	MaxOutstandingMessages int   `koanf:"CONNECTOR_PUBSUB_MAX_OUTSTANDING_MESSAGES"`

	Host                 string `koanf:"CONNECTOR_CLICKHOUSE_HOST"`
	Database             string `koanf:"CONNECTOR_CLICKHOUSE_DB"`
	User                 string `koanf:"CONNECTOR_CLICKHOUSE_USER"`
	Password             string `koanf:"CONNECTOR_CLICKHOUSE_PASSWORD"`
	Table                string `koanf:"CONNECTOR_CLICKHOUSE_TABLE"`
	WriteIntervalSeconds uint8  `koanf:"CONNECTOR_CLICKHOUSE_WRITE_INTERVAL_SECONDS"`
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
		"CONNECTOR_DEBUG": DEFAULT_DEBUG,

		"CONNECTOR_PUBSUB_IDLE_TIMEOUT_SECONDS":     DEFAULT_PUBSUB_IDLE_TIMEOUT_SECONDS,
		"CONNECTOR_PUBSUB_MAX_OUTSTANDING_MESSAGES": DEFAULT_PUBSUB_MAX_OUTSTANDING_MESSAGES,

		"CONNECTOR_CLICKHOUSE_DB":                     DEFAULT_CLICKHOUSE_DB,
		"CONNECTOR_CLICKHOUSE_USER":                   DEFAULT_CLICKHOUSE_USER,
		"CONNECTOR_CLICKHOUSE_WRITE_INTERVAL_SECONDS": DEFAULT_CLICKHOUSE_WRITE_INTERVAL_SECONDS,
	}, "."), nil)
	if err != nil {
		panic(fmt.Errorf("error while loading config defaults: %w", err))
	}
}

func mustCheckFileFlag() string {
	var fFlag = flag.String("f", "", "Path to the configuration YAML file")

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
	err := k.Load(env.Provider("CONNECTOR_", ".", nil), nil)
	if err != nil {
		panic(fmt.Errorf("error while loading env vars: %w", err))
	}
}

func mustLoadCloudRunEnv(c *Config) {
}
