package main

import (
	"github.com/lilianmartinsfritzen/home-broker/go/internal/market/transformer"
	"encoding/json"
	"github.com/lilianmartinsfritzen/home-broker/go/internal/market/dto"
	"github.com/lilianmartinsfritzen/home-broker/go/internal/infra/kafka"
	"sync"
	"github.com/lilianmartinsfritzen/home-broker/go/internal/market/entity"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	ordersIn := make(chan *entity.Order)
	ordersOut := make(chan *entity.Order)
	ws := &sync.WaitGroup{}
	defer ws.Wait()

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
		if err != nil {
			fmt.Println(err)
		}
		producer.Publish(outputJson, []byte("orders"), "output")
	}
}