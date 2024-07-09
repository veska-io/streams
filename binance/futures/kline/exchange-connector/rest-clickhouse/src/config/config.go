package config

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	DEFAULT_DEBUG                       = false
	DEFAULT_CONSUMER_TASK_QUANT_SECONDS = 60 * 60 * 1000
	DEFAULT_CONSUMER_RPS                = 7

	DEFAULT_PRODUCER_DATABASE               = "default"
	DEFAULT_PRODUCER_USER                   = "default"
	DEFAULT_PRODUCER_WRITE_INTERVAL_SECONDS = 3
)

type Config struct {
	Debug bool `koanf:"debug"`

	Consumer ConsumerConfig `koanf:"consumer"`
	Producer ProducerConfig `koanf:"producer"`
}

type ConsumerConfig struct {
	Markets []string  `koanf:"markets"`
	Start   time.Time `koanf:"start"`
	End     time.Time `koanf:"end"`

	Rps              uint8  `koanf:"rps"`
	TaskQuantSeconds uint32 `koanf:"task_quant_seconds"`
}

type ProducerConfig struct {
	Host                 string `koanf:"host"`
	Database             string `koanf:"database"`
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

	if len(c.Consumer.Markets) == 0 {
		populateMarkets(&c)
	}

	return &c
}

func mustLoadDefaults(k *koanf.Koanf) {
	now := time.Now().UTC()

	end := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC)
	start := end.Add(-1 * time.Duration(time.Hour))

	err := k.Load(confmap.Provider(map[string]interface{}{
		"debug": DEFAULT_DEBUG,

		"consumer.start": start,
		"consumer.end":   end,

		"consumer.rps":                DEFAULT_CONSUMER_RPS,
		"consumer.task_quant_seconds": DEFAULT_CONSUMER_TASK_QUANT_SECONDS,

		"producer.database":               DEFAULT_PRODUCER_DATABASE,
		"producer.user":                   DEFAULT_PRODUCER_USER,
		"producer.write_interval_seconds": DEFAULT_PRODUCER_WRITE_INTERVAL_SECONDS,
	}, "."), nil)
	if err != nil {
		panic(fmt.Errorf("error while loading config defaults: %w", err))
	}
}

func mustCheckFileFlag() string {
	var fFlag = flag.String("ff", "", "Path to the configuration YAML file")

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

func populateMarkets(cfg *Config) {
	client := binance.NewFuturesClient("", "")
	exchangeInfo, err := client.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		panic(err)
	}

	for _, s := range exchangeInfo.Symbols {
		if !strings.ContainsAny(s.Symbol, "_") {
			cfg.Consumer.Markets = append(cfg.Consumer.Markets, s.Symbol)
		}
	}
}
