package main

import (
	"encoding/hex"
	"fmt"
)

func main() {
	magicString := "Obj\x01"
	magicBytes := []byte(magicString)
	bytes, _ := hex.DecodeString("C301")
	fmt.Println(bytes)
	fmt.Println(magicString)
	fmt.Println(magicBytes)
	fmt.Println(hex.EncodeToString(magicBytes))
}
