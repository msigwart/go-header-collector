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
	start := flag.Uint64("start", 0, "start block number")

	const batchSize = 10000

	flag.Parse()

	headerDB := hc.ConnectToBlockHeaderDB(*dbhost, *dbport, *dbuser, *dbpassword, *dbname)
	defer headerDB.Close()

	client, err := ethclient.Dial("https://mainnet.infura.io/v3/ab050ca98686478e9e9b06dfc3b2f069")
	if err != nil {
		log.Fatal(err)
	}

	if *start == 0 {
		*start, err = headerDB.MinBlockNumber()
	}
	end, err := headerDB.MaxBlockNumber()
	fmt.Println(start, end)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Looking for missing headers...\n")
	for blockNumber := *start; blockNumber <= end; blockNumber += batchSize {
		orphans := make(chan *types.Header)
		go headerDB.BlocksWithMissingParents(blockNumber, blockNumber + batchSize, orphans)

		for orphan := range orphans {
			hasHeader, err := headerDB.HasHeaderOfHash(orphan.ParentHash)
			if err != nil {
				log.Fatal(err)
			}
			if hasHeader {
				continue
			}
			fmt.Printf("Header %s is orphan, looking for parent...\n", orphan.Hash().Hex())
			foundParent, err := client.HeaderByHash(context.Background(), orphan.ParentHash)
			if err != nil {
				fmt.Printf("Could not find parent %s, moving header %s to orphans...\n", orphan.ParentHash.Hex(), shortHex(orphan.Hash().Hex()))
				headerDB.MoveToOrphans(orphan.Hash())
			} else {
				fmt.Printf("Parent %s found, adding...\n", foundParent.Hash().Hex())
				_, err = headerDB.InsertBlockHeader(foundParent)
				if err != nil {
					fmt.Printf("Warning: Could not insert header (%s)\n", err)
				}
			}
		}
	}

}

func shortHex(hex string) string {
	return fmt.Sprintf("%s...%s", hex[:5], hex[len(hex)-3:])
}
