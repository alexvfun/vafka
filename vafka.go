package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

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
			ok := sendPurge(string(msg.Value))
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

//SendPurge - http purge for varnish
func sendPurge(PurgeReq string) error {
	log.Println("Got request and going to purge: ", PurgeReq)
	// Create client
	client := &http.Client{}

	// Create request
	req, err := http.NewRequest("PURGE", "http://"+PurgeReq, nil)
	if err != nil {
		log.Println(err)
		return err
	}

	// Fetch Request
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return err
	}
	defer resp.Body.Close()

	// Read Response Body
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("Got response from Varnish: \n", string(respBody))
	return nil
}

func handler(w http.ResponseWriter, r *http.Request) {

	if len(r.URL.Query().Get("host")) > 0 {
		produce([]string{r.URL.Query().Get("host") + "/"})
		fmt.Fprint(w, produce([]string{r.URL.Query().Get("host") + "/"}))
	}

}

func main() {
	//produce([]string{"www.crunchit.io/index?kuku=1"})
	go func() {
		consume()
	}()

	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}
