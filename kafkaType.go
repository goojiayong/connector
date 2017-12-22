package connector

import (
	"time"
	"strconv"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type KafkaConfig struct {
	Brokers string
	GroupId string
	ConsumerTopic string
	ProducerTopic string
	consumerMode cluster.ConsumerMode
	cmessages chan sarama.ConsumerMessage
	pmessages chan string
	cdone     chan bool
	pdone     chan bool
}

func (cfg *KafkaConfig)closeConsumer() {
	cfg.cdone <- true
}

func (cfg *KafkaConfig)closeProducer() {
	close(cfg.pmessages)
}

func NewKafkaConfig() *KafkaConfig {
	cfg := &KafkaConfig{}
	cfg.Brokers = "localhost:9092"
	cfg.ProducerTopic = "testkafka"
	cfg.ConsumerTopic = "testkafka"
	cfg.GroupId = "kafkapc-consumer-" + strconv.FormatInt(time.Now().Unix(), 10)
	cfg.consumerMode = cluster.ConsumerModeMultiplex
	cfg.cmessages    = make(chan sarama.ConsumerMessage, 10)
	cfg.pmessages    = make(chan string, 10)
	cfg.cdone        = make(chan bool)
	cfg.pdone        = make(chan bool)

	return cfg
}
