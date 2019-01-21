package kacos

import (
	"io/ioutil"
	"log"
	"vafka/pkg/vardsc"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	yaml "gopkg.in/yaml.v2"
)

type KafkaRunner struct {
	DC              string `yaml:"DC"`
	Server          string `yaml:"Server"`
	Group           string `yaml:"Group"`
	AutoOffsetReset string `yaml:"AutoOffsetReset"`
	Topic           string `yaml:"Topic"`
}

func (k *KafkaRunner) NewKafkaRunner(conf string) *KafkaRunner {

	yamlFile, err := ioutil.ReadFile(conf)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, k)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	log.Println("Kafka producer configured from ", conf)
	return k
}

func (k *KafkaRunner) Produce(CleanTargets []string) string {
	var resp string
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": k.Server})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					//log.Printf("Delivery failed: %v\n", ev.TopicPartition)
					resp = "Delivery failed: "
				} else {
					//log.Printf("Delivered message to %v\n", ev.TopicPartition)
					resp = "Delivered message"
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	//topic := "CacheInvalidate"
	for _, word := range CleanTargets {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &k.Topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
	return resp
}

func (k *KafkaRunner) Consume(myVarnish vardsc.VarnishCluster) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": k.Server,
		"group.id":          k.Group,
		"auto.offset.reset": k.AutoOffsetReset,
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{k.Topic, "^aRegex.*[Tt]opic"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			log.Println("Got message - ", string(msg.Value))
			ok := myVarnish.Purge(string(msg.Value))
			if ok != nil {
				log.Printf("Purge error %v\n", ok)
			}
		} else {
			// The client will automatically try to recover from all errors.
			log.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
