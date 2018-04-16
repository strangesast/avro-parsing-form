package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/linkedin/goavro"
	"io/ioutil"
	"os"
)

type user struct {
	Name           string `json:"name"`
	FavoriteNumber int    `json:"favorite_number"`
	FavoriteColor  string `json:"favorite_color"`
}

func getHeader(schema []byte) [34]byte {
	var header [34]byte

	leadBytes, err := hex.DecodeString("C301")
	if err != nil {
		panic(err)
	}
	copy(header[:2], leadBytes)

	fingerprint := sha256.Sum256(schema)
	copy(header[2:], fingerprint[0:])

	return header
}

func main() {
	//fmt.Println("Hello, world!")

	content, err := ioutil.ReadFile("./src/main/resources/user_parsing-form.avsc")
	if err != nil {
		panic(err)
	}

	codec, err := goavro.NewCodec(string(content))
	if err != nil {
		panic(err)
	}

	user1 := user{
		Name:           "Sam",
		FavoriteNumber: 100,
	}
	auser1 := map[string]interface{}{
		"name":            user1.Name,
		"favorite_number": goavro.Union("int", user1.FavoriteNumber),
		"favorite_color":  nil, //goavro.Union("string", user1.FavoriteColor),
	}
	//pprint(auser1)

	bytes, err := codec.BinaryFromNative(nil, auser1)
	if err != nil {
		panic(err)
	}

	// wtffff need to add fake header
	header := getHeader(content[:len(content)-1])

	fmt.Println(header)

	abytes := append(header[0:], bytes...)
	err = ioutil.WriteFile("./user-go.avro", abytes, 0644)
	if err != nil {
		panic(err)
	}

	//fmt.Println(bytes)

	f, err := os.Open("./users.avro")
	if err != nil {
		panic(err)
	}

	br := bufio.NewReader(f)

	ocfr, err := goavro.NewOCFReader(br)
	if err != nil {
		panic(err)
	}

	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			panic(err)
		}
		pprint(datum)
	}
	if err = ocfr.Err(); err != nil {
		panic(err)
	}

}

func pprint(obj interface{}) {
	result, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
