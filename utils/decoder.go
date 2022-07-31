package utils

import "github.com/mitchellh/mapstructure"

type deathHeader struct {
	Count       int      `json:"count"`
	Exchange    string   `json:"exchange"`
	RoutingKeys []string `json:"routing-keys"`
	Queue       string   `json:"queue"`
}

type header struct {
	XDeath []deathHeader `json:"x-death"`
	XRetry int           `json:"x-retry"`
}

func DecodeHeaders(input interface{}) (*header, error) {
	output := &header{}
	cfg := &mapstructure.DecoderConfig{
		Metadata: nil,
		Result:   &output,
		TagName:  "json",
	}
	decoder, err := mapstructure.NewDecoder(cfg)
	if err != nil {
		return output, err
	}
	err = decoder.Decode(input)
	return output, err
}
