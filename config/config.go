package config

import (
	"fmt"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/Yalm/go-dead-letter/utils"
	"gopkg.in/yaml.v3"
)

type RedeliveryPolicy struct {
	MaximumRedeliveries int    `yaml:"maximumRedeliveries"`
	RedeliveryDelay     int    `yaml:"redeliveryDelay"`
	Exchange            string `yaml:"exchange"`
}

type RedeliveryPolicyEntries struct {
	RedeliveryPolicy map[string]RedeliveryPolicy `yaml:"redeliveryPolicy"`
}

type RedeliveryConfig struct {
	RedeliveryPolicyEntries RedeliveryPolicyEntries `yaml:"redeliveryPolicyEntries"`
	DefaultEntry            RedeliveryPolicy        `yaml:"defaultEntry"`
}

func setDefaultValue(c *RedeliveryConfig) {
	defaultMaximumRedeliveries := utils.GetIntenv("DEFAULT_MAXIMUM_REDELIVERIES", "5")
	defaultRedeliveryDelay := utils.GetIntenv("DEFAULT_REDELIVERY_DELAY", "5000")
	defaultExchange := os.Getenv("DEFAULT_EXCHANGE")
	c.DefaultEntry = RedeliveryPolicy{MaximumRedeliveries: defaultMaximumRedeliveries, RedeliveryDelay: defaultRedeliveryDelay, Exchange: defaultExchange}
}

func ReadConf(filename string) (*RedeliveryConfig, error) {
	c := &RedeliveryConfig{}

	if len(filename) < 1 {
		setDefaultValue(c)
		return c, nil
	}

	ioutil.ReadFile(filename)
	buf, err := ioutil.ReadFile(filename)

	if os.IsNotExist(err) {
		log.Warn("File %s not found assigning default values", filename)
		setDefaultValue(c)
		return c, nil
	}

	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(buf, c)
	if err != nil {
		return nil, fmt.Errorf("in file %q: %v", filename, err)
	}

	if c.DefaultEntry == (RedeliveryPolicy{}) {
		setDefaultValue(c)
	}

	return c, nil
}
