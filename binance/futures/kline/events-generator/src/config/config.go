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
	DEFAULT_DEBUG = false
)

type Config struct {
	Debug bool `koanf:"debug"`

	Consumer ConsumerConfig `koanf:"consumer"`
	Producer ProducerConfig `koanf:"producer"`
}

type ConsumerConfig struct {
	Start time.Time `koanf:"start"`
	End   time.Time `koanf:"end"`

	Host     string `koanf:"host"`
	Port     uint32 `koanf:"port"`
	Database string `koanf:"database"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
}

type ProducerConfig struct {
	ProjectId string `koanf:"project_id"`
	TopicId   string `koanf:"topic_id"`
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
		"debug": DEFAULT_DEBUG,

		"consumer.start": start,
		"consumer.end":   end,
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
