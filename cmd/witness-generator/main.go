package main

import (
	"flag"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	hc "github.com/msigwart/header-collector"
	"github.com/pantos-io/go-testimonium/ethereum/ethash"
	"github.com/pantos-io/go-testimonium/mtree"
	"golang.org/x/crypto/sha3"
	"math"
	"time"
)

const batchSize = 10000

func coordinator(headerDb *hc.BlockHeaderDB, start uint64, end uint64, jobs chan<- *types.Header, done chan<- bool) {
	startingBlockNumber := start
	endingBlockNumber := end
	if start == 0 {
		minBlockNumberWithoutWitness, err := headerDb.MinBlockNumberWithoutWitness()
		if err != nil {
			fmt.Printf("Coordinator: %s, stopping...\n", err)
			close(jobs)
			done <- true
		}
		startingBlockNumber = minBlockNumberWithoutWitness
	}

	if end == 0 {
		// if no end block is specified the ending block is determined by the highest block number without witness data
		// this has to reevaluated on each loop iteration as this block might change
		maxBlockNumberWithoutWitness, err := headerDb.MaxBlockNumberWithoutWitness()
		if err != nil {
			fmt.Printf("Coordinator: %s, stopping...\n", err)
			close(jobs)
			done <- true
			return
		}
		endingBlockNumber = maxBlockNumberWithoutWitness
	}

	fmt.Printf("Coordinator: looking for headers without witness data (headers %d to %d)...\n", startingBlockNumber, endingBlockNumber)
	for i := startingBlockNumber; i <= endingBlockNumber; i += batchSize {
		headers := make(chan *types.Header)
		go headerDb.HeadersWithoutWitness(i, i + batchSize, headers)
		count := 0
		for header := range headers {
			jobs <- header
			count++
		}
		fmt.Printf("Coordinator: found %d headers...\n", count)
	}
}

func worker(id int, headerDb *hc.BlockHeaderDB, jobs <-chan *types.Header) {
	var dagTree *mtree.DagTree
	var currentEpoch float64 = 0
	for header := range jobs {

		newEpoch := math.Floor(float64(header.Number.Uint64() / 30000))
		if newEpoch != currentEpoch {
			currentEpoch = newEpoch
			dagTree = nil
		}
		fmt.Printf("Worker %d: generating witness data for header %s (height %d, epoch %.0f)...\n", id, header.Hash().Hex(), header.Number, currentEpoch)
		startTime := time.Now()
		if header.Hash() == (common.Hash{}) {
			fmt.Printf("Worker %d: empty block header, skipping...\n", id)
			continue
		}
		// get DAG and compute dataSetLookup and witnessForLookup
		blockMetaData := ethash.NewBlockMetaData(header.Number.Uint64(), header.Nonce.Uint64(), sealHash(header))
		if dagTree != nil {
			blockMetaData.DagTree = dagTree
		}
		dataSetLookup := blockMetaData.DAGElementArray()
		witnessForLookup := blockMetaData.DAGProofArray()
		dagTree = blockMetaData.DagTree
		headerDb.AddWitnessDataForHeader(header, dataSetLookup, witnessForLookup)
		endTime := time.Now()
		fmt.Printf("Worker %d: done (time: %.2f min).\n", id, endTime.Sub(startTime).Minutes())
	}
}

func main() {
	workers := flag.Uint("workers", 5, "number of workers")
	start := flag.Uint64("start", 0, "starting block")
	end := flag.Uint64("end", 0, "end block")
	dbhost := flag.String("dbhost", "localhost", "database host")
	dbport := flag.Uint("dbport", 5432, "database port")
	dbname := flag.String("dbname", "blockheader", "database name")
	dbuser := flag.String("dbuser", "postgres", "database user")
	dbpassword := flag.String("dbpassword", "postgres", "database password")

	flag.Parse()

	headerDB := hc.ConnectToBlockHeaderDB(*dbhost, *dbport, *dbuser, *dbpassword, *dbname)
	defer headerDB.Close()

	jobs := make(chan *types.Header, batchSize)
	done := make(chan bool)

	fmt.Printf("*** Starting witness data generation ***\n")

	// start workers
	for w := 1; w <= int(*workers); w++ {
		go worker(w, headerDB, jobs)
	}

	// start coordinator
	go coordinator(headerDB, *start, *end, jobs, done)

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
