package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	hc "github.com/msigwart/header-collector"
	"github.com/pantos-io/go-testimonium/ethereum/ethash"
	"golang.org/x/crypto/sha3"
	"io"
	"log"
	"os"
	"time"
)

const batchSize = 10000

type job struct {
	Header *types.Header
	//BufferedDag *ethash.BufferedDag
}

var dag *ethash.BufferedDag = &ethash.BufferedDag{
									Buf: make([]byte, 4*1024*1024*1024),
									Indices: make([]uint32, 4*1024*1024*1024/128),
									IndexCnt: 0,
							  }

func loadDAG(datasetPath string/*, dag *ethash.BufferedDag*/) {
	var f *os.File
	var err error
	for {
		f, err = os.Open(datasetPath)
		if err == nil {
			break
		} else {
			fmt.Printf("Reading DAG file %s failed with %s. Retry in 10s...\n", datasetPath, err.Error())
			time.Sleep(10 * time.Second)
		}
	}
	r := bufio.NewReader(f)
	buf := [128]byte{}
	// ignore first 8 bytes magic number at the beginning
	// of dataset. See more at https://gopkg.in/ethereum/wiki/wiki/Ethash-DAG-Disk-Storage-Format
	_, err = io.ReadFull(r, buf[:8])
	if err != nil {
		log.Fatal(err)
	}
	var i uint32 = 0
	for {
		//n, err := io.ReadFull(r, buf[:128])
		n, err := io.ReadFull(r, dag.Buf[i*128:(i+1)*128])
		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if n != 128 {
			log.Fatal("Malformed dataset")
		}
		dag.Indices[dag.IndexCnt] = i
		dag.IndexCnt++

		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		i++
	}
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

	ethash.MakeDAG(startingBlockNumber, ethash.DefaultDir)
	pathToDAG := ethash.PathToDAG(startingBlockNumber/30000, ethash.DefaultDir)

	fmt.Println("load DAG File...")
	loadDAG(pathToDAG, /*&dag*/)
	fmt.Println("Generate Witnesses...")


	for i := startingBlockNumber; i <= endingBlockNumber; i += batchSize {
		headers := make(chan *types.Header)
		endBlock := i + batchSize
		if endBlock > endingBlockNumber {
			endBlock = endingBlockNumber
		}
		go headerDb.HeadersWithoutWitness(i, endBlock, headers)
		count := 0
		for header := range headers {
			job := job{header, /*&dag*/}
			jobs <- job
			count++
		}

		fmt.Printf("Coordinator: found %d headers...\n", count)
	}
}

func worker(id int, headerDb *hc.BlockHeaderDB, jobs <-chan job) {
	for j := range jobs {

		fmt.Printf("Worker %d: generating witness data for header %s (height %d)...\n", id, j.Header.Hash().Hex(), j.Header.Number)
		startTime := time.Now()
		if j.Header.Hash() == (common.Hash{}) {
			fmt.Printf("Worker %d: empty block header, skipping...\n", id)
			continue
		}

		// Compute dataSetLookup and witnessForLookup
		blockMetaData := ethash.NewBlockMetaData(j.Header.Number.Uint64(), j.Header.Nonce.Uint64(), sealHash(j.Header))
		blockMetaData.BuildDagTree(dag)
		dataSetLookup := blockMetaData.DAGElementArray()
		witnessForLookup := blockMetaData.DAGProofArray()

		fmt.Printf("Worker %d: dataSetLookup: %s, witnessForLookup: %s\n", id, dataSetLookup[0], witnessForLookup[0])

		var rowsAffected int64
		var err error
		for {
			rowsAffected, err = headerDb.AddWitnessDataForHeader(j.Header, dataSetLookup, witnessForLookup)
			if err == nil {
				break
			}
			log.Println(err)
			log.Println("Trying again...")
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
