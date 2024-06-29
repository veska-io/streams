package config

import (
	"flag"
	"fmt"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	DEFAULT_DEBUG      = false
	TASK_QUANT_SECONDS = 60 * 60 * 24
	RPS                = 7
)

type Config struct {
	Debug bool `koanf:"CONNECTOR_DEBUG"`

	Markets []string  `koanf:"CONNECTOR_MARKETS"`
	Start   time.Time `koanf:"CONNECTOR_START"`
	End     time.Time `koanf:"CONNECTOR_END"`

	Rps              uint8  `koanf:"CONNECTOR_RPS"`
	TaskQuantSeconds uint32 `koanf:"CONNECTOR_TASK_QUANT_SECONDS"`

	PubSubProjectId string `koanf:"CONNECTOR_PUBSUB_PROJECT_ID"`
	PubSubTopic     string `koanf:"CONNECTOR_PUBSUB_TOPIC"`
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
	now := time.Now().UTC()

	end := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC)
	start := end.Add(-1 * time.Duration(time.Hour))

	err := k.Load(confmap.Provider(map[string]interface{}{
		"CONNECTOR_DEBUG": DEFAULT_DEBUG,

		"CONNECTOR_START": start,
		"CONNECTOR_END":   end,

		"CONNECTOR_RPS":                RPS,
		"CONNECTOR_TASK_QUANT_SECONDS": TASK_QUANT_SECONDS,
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
