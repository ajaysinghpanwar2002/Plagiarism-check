package monitoring

import (
	"fmt"
	"time"

	"github.com/cactus/go-statsd-client/v5/statsd"
)

func ConnectStatsd(host, port, prefix string) (statsd.Statter, error) {
	address := fmt.Sprintf("%s:%s", host, port)

	config := statsd.ClientConfig{
		Address:       address,
		Prefix:        prefix,
		TagFormat:     statsd.InfixComma,
		UseBuffered:   true,
		FlushInterval: 1 * time.Second,
	}

	return statsd.NewClientWithConfig(&config)
}

func Increment(name string, client statsd.Statter) {
	err := client.Inc(name, 1, 0.5)
	if err != nil {
		fmt.Println(err)
	}
}

func IncrementWithTags(name string, client statsd.Statter, language string) {
	tag := statsd.Tag{"language", language}
	err := client.Inc(name, 1, 1.0, tag)
	if err != nil {
		fmt.Println(err)
	}
}

func Timing(name string, client statsd.Statter, timing time.Time, tags ...statsd.Tag) {
	delta := time.Since(timing).Milliseconds()
	err := client.Timing(name, delta, 1.0, tags...)
	if err != nil {
		fmt.Println(err)
	}
}
