package go_header_collector

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/core/types"
	_ "github.com/lib/pq"
	"log"
)

type BlockHeaderDB struct {
	db *sql.DB
}

func (db *BlockHeaderDB) InsertBlockHeader(header *types.Header) {
	headerJson, err := json.Marshal(header)
	if err != nil {
		log.Fatal(err)
	}

	sqlStatement := `
INSERT INTO blockheader (block_hash, block_number, block_data)
VALUES ($1, $2, $3)`
	_, err = db.db.Exec(sqlStatement, header.Hash().Hex()[2:], header.Number.String(), string(headerJson))
	if err != nil {
		log.Fatal(err)
	}
}

func (db *BlockHeaderDB) Close() {
	err := db.db.Close()
	log.Fatal(err)
}

func ConnectToBlockHeaderDB(host string, port uint, user string, password string, dbname string) *BlockHeaderDB {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Successfully connected to database %s\n", dbname)
	return &BlockHeaderDB{db}
}

func (db *BlockHeaderDB) MinBlockNumberWithoutWitness() uint64 {
	sqlStatement := `SELECT MIN(block_number) FROM blockheader WHERE dataset_lookup IS NULL`
	var minBlockNumberWithoutWitness uint64
	row := db.db.QueryRow(sqlStatement)
	switch err := row.Scan(&minBlockNumberWithoutWitness); err {
	case sql.ErrNoRows:
		log.Fatal("No rows without witness data exist... nothing to do!")
	case nil:
		break
	default:
		log.Fatal(err)
	}
	return minBlockNumberWithoutWitness
}
