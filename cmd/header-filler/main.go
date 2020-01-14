package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	hc "github.com/msigwart/header-collector"
	"log"
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

	client, err := ethclient.Dial("https://mainnet.infura.io/v3/ab050ca98686478e9e9b06dfc3b2f069")
	if err != nil {
		log.Fatal(err)
	}

	start, err := headerDB.MinBlockNumber()
	end, err := headerDB.MaxBlockNumber()
	fmt.Println(start, end)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Looking for missing headers...\n")
	for blockNumber := start + 1; blockNumber <= end; blockNumber++ {
		fmt.Printf("Height %d...\r", blockNumber)
		headers := make(chan *types.Header)
		go headerDB.HeadersOfHeight(blockNumber, headers)

		for header := range headers {
			hasHeader, err := headerDB.HasHeaderOfHash(header.ParentHash)
			if err != nil {
				log.Fatal(err)
			}
			if hasHeader {
				continue
			}
			fmt.Printf("Header %s (height: %d) is missing, inserting now...\n", header.ParentHash.Hex(), blockNumber)
			missingHeader, err := client.HeaderByHash(context.Background(), header.ParentHash)
			if err != nil {
				log.Fatal(err)
			}
			headerDB.InsertBlockHeader(missingHeader)
			fmt.Printf("Successfully inserted header %s, looking for next missing header...\n", header.ParentHash.Hex())
		}
	}

}
