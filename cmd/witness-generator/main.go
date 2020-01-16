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
	"log"
	"math"
	"time"
)

const batchSize = 10000

type job struct {
	Header *types.Header
	BlockMetaData *ethash.BlockMetaData
}

func coordinator(headerDb *hc.BlockHeaderDB, start uint64, end uint64, jobs chan<- job, done chan<- bool) {
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
		endBlock := i + batchSize
		if endBlock > endingBlockNumber {
			endBlock = endingBlockNumber
		}
		go headerDb.HeadersWithoutWitness(i, endBlock, headers)
		var jobsArray []job
		var metaDataArray []*ethash.BlockMetaData
		for header := range headers {
			blockMetaData := ethash.NewBlockMetaData(header.Number.Uint64(), header.Nonce.Uint64(), sealHash(header))
			jobsArray = append(jobsArray, job{header, blockMetaData})
			metaDataArray = append(metaDataArray, blockMetaData)
		}
		ethash.BuildDagTrees(metaDataArray)
		count := 0
		for i, job := range jobsArray {
			jobs <- job
			count = i + 1
		}

		fmt.Printf("Coordinator: found %d headers...\n", count)
	}
}

func worker(id int, headerDb *hc.BlockHeaderDB, jobs <-chan job) {
	var currentEpoch float64 = 0
	for j := range jobs {

		newEpoch := math.Floor(float64(j.Header.Number.Uint64() / 30000))
		if newEpoch != currentEpoch {
			currentEpoch = newEpoch
			//dagTree = nil
		}
		fmt.Printf("Worker %d: generating witness data for header %s (height %d, epoch %f)...\n", id, j.Header.Hash().Hex(), j.Header.Number, currentEpoch)
		startTime := time.Now()
		if j.Header.Hash() == (common.Hash{}) {
			fmt.Printf("Worker %d: empty block header, skipping...\n", id)
			continue
		}
		// Compute dataSetLookup and witnessForLookup
		dataSetLookup := j.BlockMetaData.DAGElementArray()
		witnessForLookup := j.BlockMetaData.DAGProofArray()
		//dagTree = blockMetaData.DagTree
		fmt.Printf("Worker %d: dataSetLookup: %s, witnessForLookup: %s\n", id, dataSetLookup[0], witnessForLookup[0])
		rowsAffected, err := headerDb.AddWitnessDataForHeader(j.Header, dataSetLookup, witnessForLookup)
		if err != nil {
			log.Fatal(err)
		}
		endTime := time.Now()
		fmt.Printf("Worker %d: done (time: %.2f min, rows affected: %d).\n", id, endTime.Sub(startTime).Minutes(), rowsAffected)
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

	jobs := make(chan job, batchSize)
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
