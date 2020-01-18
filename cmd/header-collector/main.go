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

	wsendpoint := flag.String("wsendpoint", "ws://localhost:8546", "Ethereum websocket endpoint")
	dbhost := flag.String("dbhost", "localhost", "database host")
	dbport := flag.Uint("dbport", 5432, "database port")
	dbname := flag.String("dbname", "blockheader", "database name")
	dbuser := flag.String("dbuser", "postgres", "database user")
	dbpassword := flag.String("dbpassword", "postgres", "database password")

	flag.Parse()

	headerDB := hc.ConnectToBlockHeaderDB(*dbhost, *dbport, *dbuser, *dbpassword, *dbname)
	defer headerDB.Close()

	//client, err := ethclient.Dial("wss://mainnet.infura.io/ws/v3/ab050ca98686478e9e9b06dfc3b2f069")
	client, err := ethclient.Dial(*wsendpoint)
	if err != nil {
		log.Fatal(err)
	}

	headers := make(chan *types.Header)

	sub, err := client.SubscribeNewHead(context.Background(), headers)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case err := <-sub.Err():
			log.Fatal(err)
		case header := <-headers:
			fmt.Printf("Height: %s: %s\n", header.Number.String(), header.Hash().Hex()) // 0xbc10defa8dda384c96a17640d84de5578804945d347072e091b4e5f390ddea7f
			_, err := headerDB.InsertBlockHeader(header)
			if err != nil {
				fmt.Printf("Warning: Could not insert header (%s)\n", err)
			}
		}
	}

}
