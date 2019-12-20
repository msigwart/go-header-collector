package main

import (
	"flag"
	"fmt"
	"github.com/pantos-io/go-testimonium/ethereum/ethash"
)

func main() {
	startEpoch := flag.Uint64("start", 302, "starting epoch") // 302 epoch istanbul hardfork
	endEpoch := flag.Uint64("end", 335, "end epoch")   // 33 epochs ~ 1,000,000 blocks
	output := flag.String("out", ethash.DefaultDir, "output output")

	flag.Parse()

	fmt.Printf("*** Starting cache generation for epochs %d to %d (%d epochs) ***\n", *startEpoch, *endEpoch, *endEpoch + 1 -*startEpoch)
	fmt.Printf("Output directory: %s\n", *output)

	for i := *startEpoch; i <= *endEpoch; i++ {
		fmt.Printf("Generating cache for epoch %d...\n", i)
		ethash.MakeCache(i*30000, *output)
		ethash.MakeDataset(i*30000, *output)
	}
}
