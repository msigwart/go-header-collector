package main

import (
	"flag"
	"fmt"
	"github.com/pantos-io/go-testimonium/ethereum/ethash"
)

func main() {
	startBlock := flag.Uint64("start", 9069000, "starting block height")	// block of istanbul hardfork
	endBlock := flag.Uint64("end", 10069000, "end block height")
	directory := flag.String("out", ethash.DefaultDir, "output directory")

	flag.Parse()

	fmt.Printf("*** Starting cache generation for blocks %d to %d (%d epochs) ***\n", *startBlock, *endBlock, (*endBlock-*startBlock)/30000)
	fmt.Printf("Output directory: %s\n", *directory)

	for i := *startBlock; i < *endBlock; i += 30000 {
		fmt.Printf("Generating cache for epoch %d...\n", i/30000)
		ethash.MakeDAG(i, *directory)
	}
}
