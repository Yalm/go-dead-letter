package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type RedeliveryPolicy struct {
	MaximumRedeliveries int64 `yaml:"maximumRedeliveries"`
	RedeliveryDelay     int64 `yaml:"redeliveryDelay"`
}

type RedeliveryPolicyEntries struct {
	RedeliveryPolicy map[string]RedeliveryPolicy `yaml:"redeliveryPolicy"`
}

type RedeliveryConfig struct {
	RedeliveryPolicyEntries RedeliveryPolicyEntries `yaml:"redeliveryPolicyEntries"`
	DefaultEntry            RedeliveryPolicy        `yaml:"defaultEntry"`
}

func setDefaultValue(c *RedeliveryConfig) {
	c.DefaultEntry = RedeliveryPolicy{MaximumRedeliveries: 5, RedeliveryDelay: 5000}
}

func ReadConf(filename string) (*RedeliveryConfig, error) {
	c := &RedeliveryConfig{}
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
