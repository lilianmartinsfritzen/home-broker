package main

import (
	"encoding/json"
	"fmt"
	"sync"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/lilianmartinsfritzen/home-broker/go/internal/infra/kafka"
	"github.com/lilianmartinsfritzen/home-broker/go/internal/market/dto"
	"github.com/lilianmartinsfritzen/home-broker/go/internal/market/entity"
	"github.com/lilianmartinsfritzen/home-broker/go/internal/market/transformer"
)

func main() {
	ordersIn := make(chan *entity.Order)
	ordersOut := make(chan *entity.Order)
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	kafkaMsgChan := make(chan *ckafka.Message)
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": 	"host.docker.internal:9094",
		"group.id": 					"myGroup",
		"auto.offset.reset": 	"earliest"
	}
	producer := kafka.NewKafkaProducer(configMap)
	kafka := kafka.NewConsumer(configMap, []string{"input"})

	// com o go na frente acamos de criar uma segunda Thread
	// caso contrário nada que foi escrito abaixo dessa linha funcionaria, seria bloqueado
	go kafka.Consume(kafkaMsgChan)

	// recebe do canal do kafka, adiciona no input, processa, adiciona no output
	// e depois publica no kafka
	book := entity.NewBook(ordersIn, ordersOut, wg)

	// com o go criamos uma terceira Thread, pois existe um loop infinito dentro de Trade
	// com isso a aplicação começaria a travar nessa etapa
	go book.Trade()

	go func() {
		for msg := range kafkaMsgChan {
			wg.Add(1)
			fmt.Println(string(msg.Value))
			tradeInput := dto.TradeInput{}
			err := json.Unmarshal(msg.Value, &tradeInput)
			if err != nil {
				panic(err)
			}
			order := transformer.TransformInput(tradeInput)
			ordersIn <- order
		}
	}()

	for res := range ordersOut {
		output := transformer.TransformOutput(res)
		outputJson, err := json.MarshalIndent(output, "", "  ")
		fmt.Println(string(outputJson))
		if err != nil {
			fmt.Println(err)
		}
		producer.Publish(outputJson, []byte("orders"), "output")
	}
}
