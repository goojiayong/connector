package connector

import (
	"strings"
	"time"

	log "common-core/aclog"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

func Consumer(cfg *KafkaConfig) {
	groupID := cfg.GroupId
	config := cluster.NewConfig()
	//config.Group.Mode = cluster.ConsumerModePartitions
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	brokers := strings.Split(cfg.Brokers, ",")
	topics  := strings.Split(cfg.ConsumerTopic, ",")

	consumer, err := cluster.NewConsumer(brokers, groupID, topics, config)
	if err != nil {
		log.Error(err.Error())
		return
	}
	defer consumer.Close()

	go func() {
		for err := range consumer.Errors() {
			log.Error(err.Error())
		}
	}()

	go func() {
		for ntf := range consumer.Notifications() {
			log.Info(ntf.Type)
		}
	}()

	for exit := false; exit != true; {
		select {
		case <-cfg.cdone:
			consumer.Close()
			exit = true
		default:
		}

		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				cfg.cmessages <- *msg
				consumer.MarkOffset(msg, "")
			}
		case <-time.After(time.Second * 3):
		}
	}
}

