package main

import (
	"flag"
	"fmt"
	"net/http"
	"vafka/pkg/kacos"
	"vafka/pkg/vardsc"
)

var myVarnisConf vardsc.VarnishConf

var myKafka kacos.KafkaRunner
var vKafkaFile *string
var myVarnish vardsc.VarnishCluster

//KConsumer - kafka consumer implementation
/*func consume() {
	var myVarnish vardsc.VarnishCluster
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
}*/

func handler(w http.ResponseWriter, r *http.Request) {

	//myKafka.New(*vKafkaFile)
	if len(r.URL.Query().Get("host")) > 0 {
		//myKafka.Produce([]string{r.URL.Query().Get("host")})
		fmt.Fprint(w, myKafka.Produce([]string{r.URL.Query().Get("host")}))
	}

}

func main() {
	// getting config parameters
	vConfMethod := flag.String("vconfmethod", "conf", "Varnish topology configuration: default for fake, conf for config file, consul for consul based")
	vConfFile := flag.String("vconffile", "/Users/alexeyfy/go/src/vafka/conf/varnish.yaml", "Varnish topology config file")
	vHTTPPort := flag.String("httpport", "8080", "HTTP server port")
	vKafkaFile = flag.String("vkafkafile", "/Users/alexeyfy/go/src/vafka/conf/kafka.yaml", "Kafka config")
	myVarnisConf.Method = *vConfMethod
	myVarnisConf.ConfFile = *vConfFile

	myVarnish.New(myVarnisConf)
	
	myKafka.NewKafkaRunner(*vKafkaFile)
	myKafka.Produce([]string{"www.crunchit.io"})
	go func() {
		//consume()
		myKafka.Consume(myVarnish)
	}()

	http.HandleFunc("/", handler)
	http.ListenAndServe(":"+*vHTTPPort, nil)
}
