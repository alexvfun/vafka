package main

import (
	"flag"
	"log"
	"net/http"
	"vafka/pkg"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var myVarnisConf vardsc.VarnishConf

//Message producer for tests
func produce(CleanTargets []string) string {
	var resp string
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
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
	topic := "CacheInvalidate"
	for _, word := range CleanTargets {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
	return resp
}

//KConsumer - kafka consumer implementation
func consume() {
	myVarnish := new(vardsc.VarnishCluster)
	myVarnish.New(myVarnisConf)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"CacheInvalidate", "^aRegex.*[Tt]opic"}, nil)

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

func handler(w http.ResponseWriter, r *http.Request) {

	if len(r.URL.Query().Get("host")) > 0 {
		produce([]string{r.URL.Query().Get("host")})
		//fmt.Fprint(w, produce([]string{r.URL.Query().Get("host")}))
	}

}

func main() {
	// getting config parameters
	vConfMethod := flag.String("vconfmethod", "conf", "Varnish topology configuration: default for fake, conf for config file, consul for consul based")
	vConfFile := flag.String("vconffile", "/Users/alexeyfy/go/src/vafka/conf/varnish.yaml", "Varnish topology config file")
	vHTTPPort := flag.String("httpport", "8080", "HTTP server port")

	myVarnisConf.Method = *vConfMethod
	myVarnisConf.ConfFile = *vConfFile

	go func() {
		consume()
	}()

	http.HandleFunc("/", handler)
	http.ListenAndServe(":"+*vHTTPPort, nil)
}
