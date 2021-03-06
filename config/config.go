package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/Yalm/go-dead-letter/utils"
	"gopkg.in/yaml.v3"
)

type BackOffOptions struct {
	InitialInterval int `yaml:"initialInterval"`
	Multiplier      int `yaml:"multiplier"`
	MaxInterval     int `yaml:"maxInterval"`
}

type RedeliveryPolicy struct {
	MaximumRedeliveries int            `yaml:"maximumRedeliveries"`
	RedeliveryDelay     int            `yaml:"redeliveryDelay"`
	BackOffOptions      BackOffOptions `yaml:"backOffOptions"`
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
	c.DefaultEntry = RedeliveryPolicy{
		MaximumRedeliveries: defaultMaximumRedeliveries,
		RedeliveryDelay:     defaultRedeliveryDelay,
	}
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
		log.Printf("File %s not found assigning default values", filename)
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
