package main

import (
	"flag"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	hc "github.com/msigwart/header-collector"
	"github.com/pantos-io/go-testimonium/ethereum/ethash"
	"golang.org/x/crypto/sha3"
)

const BATCH_SIZE = 100

func coordinator(headerDb *hc.BlockHeaderDB, jobs chan<- uint64, results <-chan uint64, done chan<- bool) {
	for {
		minBlockNumber, err := headerDb.MinBlockNumberWithoutWitness()
		if err != nil {
			fmt.Printf("Coordinator: %s, stopping...\n", err)
			close(jobs)
			break
		}
		fmt.Printf("Coordinator: generating witness data for blocks %d to %d...\n", minBlockNumber, minBlockNumber+BATCH_SIZE-1)
		for i := minBlockNumber; i < minBlockNumber+BATCH_SIZE; i++ {
			jobs <- i
		}
		for r := 0; r < BATCH_SIZE; r++ {
			<-results
		}
	}
	done <- true
}

func worker(id int, headerDb *hc.BlockHeaderDB, jobs <-chan uint64, results chan<- uint64) {
	for blockNumber := range jobs {
		fmt.Printf("Worker %d: generating witness data for blocks of height %d...\n", id, blockNumber)
		headers := make(chan *types.Header)
		go headerDb.HeadersOfHeight(blockNumber, headers)

		for header := range headers {
			if header.Hash() == (common.Hash{}) {
				fmt.Printf("Worker %d: empty block header, skipping...\n", id)
				continue
			}
			fmt.Printf("Worker %d: block %s...\n", id, header.Hash().String())
			// get DAG and compute dataSetLookup and witnessForLookup
			blockMetaData := ethash.NewBlockMetaData(header.Number.Uint64(), header.Nonce.Uint64(), sealHash(header))
			dataSetLookup := blockMetaData.DAGElementArray()
			witnessForLookup := blockMetaData.DAGProofArray()
			headerDb.AddWitnessDataForHeader(header, dataSetLookup, witnessForLookup)
		}
		fmt.Printf("Worker %d: done.\n", id)
		results <- blockNumber
	}
}

func main() {
	workers := flag.Uint("workers", 5, "number of workers")
	dbhost := flag.String("dbhost", "localhost", "database host")
	dbport := flag.Uint("dbport", 5432, "database port")
	dbname := flag.String("dbname", "blockheader", "database name")
	dbuser := flag.String("dbuser", "postgres", "database user")
	dbpassword := flag.String("dbpassword", "postgres", "database password")

	flag.Parse()

	headerDB := hc.ConnectToBlockHeaderDB(*dbhost, *dbport, *dbuser, *dbpassword, *dbname)
	defer headerDB.Close()


	jobs := make(chan uint64, BATCH_SIZE)
	results := make(chan uint64, BATCH_SIZE)
	done := make(chan bool)

	fmt.Printf("*** Starting witness data generation ***\n")

	// start workers
	for w := 1; w <= int(*workers); w++ {
		go worker(w, headerDB, jobs, results)
	}

	// start coordinator
	go coordinator(headerDB, jobs, results, done)

	<-done
	fmt.Printf("*** Witness data generation done ***\n")
}

func sealHash(header *types.Header) (hash common.Hash) {
	hasher := sha3.NewLegacyKeccak256()

	_ = rlp.Encode(hasher, []interface{}{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra,
	})
	hasher.Sum(hash[:0])
	return hash
}
