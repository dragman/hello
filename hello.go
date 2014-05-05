package main

import (
	"code.google.com/p/goprotobuf/proto"
	//"fmt"
	gl "glacier.pb"
	"io/ioutil"
	"log"
	//tb "tbricks.pb"
)

var filename string = "/Users/dragos/Programming/python/glacier/test.bin"

func main() {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal("Problem reading file: ", err)
	}

	pos := 0
	time := uint64(0)

ReadLoop:
	for {
		x, bytesRead := proto.DecodeVarint(data[pos:])
		if bytesRead <= 0 {
			log.Fatal("Failed to read next varint! pos = ", pos)
		}
		pos += bytesRead
		nextMessageLength := int(int32(x))

		block := &gl.Block{}
		proto.Unmarshal(data[pos:pos+nextMessageLength], block)

		time += block.GetTimeDelta()

		switch block.GetType() {
		case gl.Block_STREAM_EVENT:
			log.Println("Event: ", block.String())
			if block.GetEvent().GetType() == gl.Event_END {
				log.Println("Finished!")
				log.Printf("Final offset = %v hours", time/1000000/60/60)
				break ReadLoop
			}
		case gl.Block_ORDERBOOK_UPDATE:
			// log.Println(block.String())
		case gl.Block_UNKNOWN:
			panic("Didn't know how to deal with UNKNOWN block.")
		}

		pos += nextMessageLength
	}
}
