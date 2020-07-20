package main

import (
    "fmt"
		"net/http"
		"log"
		"bytes"
		"math/rand"

		"github.com/Shopify/sarama"
		"github.com/kelseyhightower/envconfig"
		"knative.dev/eventing-contrib/kafka"
	)

type envConfig struct {
	Topic   string `envconfig:"KAFKA_TOPIC" required:"true"`
	BootstrapServers []string `envconfig:"KAFKA_BOOTSTRAP_SERVERS" required:"true"`
	Key     string `envconfig:"KAFKA_KEY" required:"true"`
	Headers map[string]string `envconfig:"KAFKA_HEADERS" required:"true"`
	Value   string
}

func main() {
	// Start an HTTP Server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var isAsync bool
		asyncHeader := r.Header.Get("Prefer")
		if asyncHeader == "respond-async" {
			isAsync = true
			fmt.Println("RESPOND ASYNC IS TRUE")
		}
		if !isAsync {
			fmt.Println("BMV NO RESPOND ASYNC")
			// h.NextHandler.ServeHTTP(w, r)
		} else {
			internalURL := "http://" + r.Host + r.URL.String()
			buf := new(bytes.Buffer)
			buf.ReadFrom(r.Body)
			bodyStr := buf.String()
			reqData := `{"method":"` + r.Method + `","url":"` + internalURL +`","body":"` + bodyStr + `"}`
			var s envConfig
			err := envconfig.Process("", &s) // BMV TODO: how can we process just a subset of env, providing "kafka" maybe?
			if err != nil {
				log.Fatal(err.Error())
			}

			ctx := r.Context()
			// Create a Kafka client from our Binding.
			client, err := kafka.NewProducer(ctx)
			if err != nil {
				log.Fatal(err.Error())
			}
			producer, err := sarama.NewSyncProducerFromClient(client)
			if err != nil {
				log.Fatal(err.Error())
			}

			// Send the message this Job was created to send.
			headers := make([]sarama.RecordHeader, 0, len(s.Headers))
			for k, v := range s.Headers {
				headers = append(headers, sarama.RecordHeader{
					Key:   []byte(k),
					Value: []byte(v),
				})
			}
			partitionnum := rand.Int31n(3)
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic:   s.Topic,
				// Key:     sarama.StringEncoder(s.Key), //BMV TODO: What does key do?
				Partition: partitionnum,
				Value:   sarama.StringEncoder(reqData),
				Headers: headers,
			})
			if err != nil {
				log.Fatal(err.Error())
			} else {
				log.Print(partition)
				log.Print(offset)
				w.WriteHeader(http.StatusAccepted)
			}

			// BMV TODO: do we need to close any connections or does writing the header handle this?
			// h.NextHandler.ServeHTTP(w, r)

		}
	})
	log.Fatal(http.ListenAndServe(":8012", nil))
}