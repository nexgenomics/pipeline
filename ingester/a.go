package main


import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)


/****
main
****/

func main() {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"192.168.0.222:9092"},
		GroupID: "qdrant",
		Topic: "ng-sentences",
		MinBytes: 1,
		MaxBytes: 10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()

	ctx := context.Background()

	log.Println ("Listening...")

	for {
		if m,e := r.ReadMessage (ctx); e == nil {
			log.Printf ("%s, %s", string(m.Key), string(m.Value))
		} else {
			log.Printf ("%s",e)
		}
	}

}


