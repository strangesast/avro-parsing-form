package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/fatih/structs"
	"github.com/linkedin/goavro"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	//"time"
	"encoding/hex"
)

func fingerprint(data []byte) [8]byte {
	// need table for schema fingerprinting
	if !tableInitialized {
		initTable()
	}
	fp := empty
	for _, b := range data {
		fp = int(uint(fp)>>8) ^ table[(fp^int(b))&0xff]
	}
	var result [8]byte
	for i := 0; i < 8; i++ {
		result[i] = byte(fp)
		fp = fp >> 8
	}
	return result
}

func initTable() {
	for i := 0; i < 256; i++ {
		fp := i
		for j := 0; j < 8; j++ {
			mask := -(fp & 1)
			fp = int(uint(fp)>>1) ^ (empty & mask)
		}
		table[i] = fp
	}
	tableInitialized = true
}

func buildHeader(schema []byte) []byte {
	/* create header from [2]byte prefix and [8]byte fingerprint */
	header := [10]byte{195, 1} // C3 01

	fp := fingerprint(schema)
	copy(header[2:], fp[0:])

	return header[0:]
}

//var empty int = -4513414715797952619 // also 0xc15d213aa4d7a795
var emptyUnsigned = uint(0xc15d213aa4d7a795)
var empty = int(emptyUnsigned)

var table [256]int
var tableInitialized = false

func randomRange(min, max int) int {
	// rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

type temp struct {
	Date  int64   `structs:"date"`
	Value float64 `structs:"value"`
}

type sensor struct {
	ID string `structs:"_id"`
}

func createSensors(count int) []sensor {
	var sensors []sensor
	for i := 0; i < count; i++ {
		sensors = append(sensors, sensor{"sensor-" + strconv.Itoa(i)})
	}
	return sensors
}

func builder(avroPath string) func(interface{}) []byte {
	if !strings.Contains(avroPath, "parsing-form") {
		panic(`avro source file should contain "parsing-form" if it is in parsing form (as it should be)`)
	}
	content, err := ioutil.ReadFile(avroPath)
	if err != nil {
		panic(err)
	}
	header := buildHeader(content)

	/* encode avro message from interface */
	codec, err := goavro.NewCodec(string(content))
	if err != nil {
		panic(err)
	}

	return func(obj interface{}) []byte {
		bytes, err := codec.BinaryFromNative(nil, structs.Map(obj))
		if err != nil {
			panic(err)
		}

		body := append(header[0:], bytes...)

		return body
	}
}

func setupConnection() chan *sarama.ProducerMessage {
	/* setup connection to kafka */
	brokers := []string{"localhost:9092"}

	config := sarama.NewConfig()
	config.ClientID = "sarama-client"
	config.Version = sarama.V1_0_0_0
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
	}()

	ch := make(chan *sarama.ProducerMessage)

	go func() {
	Loop:
		for {
			message := <-ch
			/* send message, block for response or error */
			select {
			case producer.Input() <- message:
				fmt.Println("success!", message, message.Offset, message.Timestamp.Unix())
			case err := <-producer.Errors():
				fmt.Println("failure", err)
				break Loop
			}
		}
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	return ch
}

func main() {
	content, _ := ioutil.ReadFile("../avro-schemas/point_parsing-form.avsc")
	header := buildHeader(content)
	fmt.Println(hex.EncodeToString(header))
	/*
		messageChannel := setupConnection()

		tempMessageBuilder := builder("../avro-schemas/temp_parsing-form.avsc")
		pointMessageBuilder := builder("../avro-schemas/point_parsing-form.avsc")

		// add sensors
		sensors := createSensors(1)

		for _, sensor := range sensors {
			body := pointMessageBuilder(sensor)
			msg := &sarama.ProducerMessage{
				Topic: "points",
				Key:   sarama.StringEncoder(sensor.ID),
				Value: sarama.ByteEncoder(body),
			}
			messageChannel <- msg
		}

		// produce some random fluctuations

		for i := 0; i < 1; i++ {
			temp := temp{time.Now().UnixNano() / 1e6, rand.Float64()}

			fmt.Printf("%+v\n", temp)

			body := tempMessageBuilder(temp)

			id := "some-sensor-" + strconv.Itoa(randomRange(0, 100))
			msg := &sarama.ProducerMessage{
				Topic: "temps",
				Key:   sarama.StringEncoder(id),
				Value: sarama.ByteEncoder(body),
			}

			messageChannel <- msg

			time.Sleep(time.Millisecond)
		}
	*/
}
