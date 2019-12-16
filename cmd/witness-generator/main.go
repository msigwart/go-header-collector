package main

import (
	"flag"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	hc "github.com/msigwart/header-collector"
	"golang.org/x/crypto/sha3"
	"os"
)

func main() {
	dbhost := flag.String("dbhost", "localhost", "database host")
	dbport := flag.Uint("dbport", 5432, "database port")
	dbname := flag.String("dbname", "blockheader", "database name")
	dbuser := flag.String("dbuser", "postgres", "database user")
	dbpassword := flag.String("dbpassword", "postgres", "database password")

	flag.Parse()

	headerDB := hc.ConnectToBlockHeaderDB(*dbhost, *dbport, *dbuser, *dbpassword, *dbname)
	defer headerDB.Close()

	minBlockNumber := headerDB.MinBlockNumberWithoutWitness()
	fmt.Printf("Starting witness generation from block %d...\n", minBlockNumber)
	os.Exit(0)
	////client, err := ethclient.Dial("wss://mainnet.infura.io/ws/v3/ab050ca98686478e9e9b06dfc3b2f069")
	//client, err := ethclient.Dial("ws://localhost:8546")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//headers := make(chan *types.Header)
	//
	//sub, err := client.SubscribeNewHead(context.Background(), headers)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//for {
	//	select {
	//	case err := <-sub.Err():
	//		log.Fatal(err)
	//	case header := <-headers:
	//		fmt.Printf("Height: %s: %s\n", header.Number.String(), header.Hash().Hex()) // 0xbc10defa8dda384c96a17640d84de5578804945d347072e091b4e5f390ddea7f
	//		headerDB.InsertBlockHeader(header)
	//
	//		fmt.Println("create DAG, compute dataSetLookup and witnessForLookup")
	//		// get DAG and compute dataSetLookup and witnessForLookup
	//		blockMetaData := ethash.NewBlockMetaData(header.Number.Uint64(), header.Nonce.Uint64(), sealHash(header))
	//		dataSetLookup := blockMetaData.DAGElementArray()
	//		witnessForLookup := blockMetaData.DAGProofArray()
	//		fmt.Printf("dataSetLookup: %s\n", dataSetLookup)
	//		fmt.Printf("witnessForLookup: %s\n", witnessForLookup)
	//	}
	//}
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
