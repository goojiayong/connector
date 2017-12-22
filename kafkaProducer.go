package connector

import (
	"strings"
	"time"

	log "common-core/aclog"
	"github.com/Shopify/sarama"
)

func Producer(cfg *KafkaConfig) {
	brokers := strings.Split(cfg.Brokers, ",")
	topics  := strings.Split(cfg.ProducerTopic, ",")
	topic := topics[0]

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 3 * time.Second
	p, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Error(err.Error())
		return
	}
	defer p.Close()

	go func(p sarama.AsyncProducer) {
		errors := p.Errors()
		success := p.Successes()
		for {
			select {
			case err := <-errors:
				if err != nil {
					log.Error(err.Error())
				}
			case <-success:
			}
		}
	}(p)

	for exit := false; exit != true; {
		select {
		case message, ok := <- cfg.pmessages:
			if !ok {
				exit = true
				break
			}
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(message),
			}
			p.Input() <- msg
		}
	}
}
