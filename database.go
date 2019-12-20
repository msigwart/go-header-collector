package go_header_collector

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"log"
	"math/big"
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

func (db *BlockHeaderDB) MinBlockNumberWithoutWitness() (uint64, error) {
	sqlStatement := `SELECT MIN(block_number) FROM blockheader WHERE dataset_lookup IS NULL`
	var minBlockNumberWithoutWitness uint64
	row := db.db.QueryRow(sqlStatement)
	switch err := row.Scan(&minBlockNumberWithoutWitness); err {
	case sql.ErrNoRows:
		fmt.Println()
		return 0, fmt.Errorf("no rows without witness data exist")
	case nil:
		break
	default:
		return 0, err
	}
	return minBlockNumberWithoutWitness, nil
}

func (db *BlockHeaderDB) MaxBlockNumberWithoutWitness() (uint64, error) {
	sqlStatement := `SELECT MAX(block_number) FROM blockheader WHERE dataset_lookup IS NULL`
	var maxBlockNumberWithoutWitness uint64
	row := db.db.QueryRow(sqlStatement)
	switch err := row.Scan(&maxBlockNumberWithoutWitness); err {
	case sql.ErrNoRows:
		return 0, fmt.Errorf("no rows without witness data exist")
	case nil:
		break
	default:
		return 0, err
	}
	return maxBlockNumberWithoutWitness, nil
}

func (db *BlockHeaderDB) HasHeadersWithoutWitnessOfHeight(height uint64) bool {
	sqlStatement := `SELECT COUNT(1) FROM blockheader WHERE block_number = $1 AND dataset_lookup IS NULL`
	row := db.db.QueryRow(sqlStatement, height)
	var count uint
	switch err := row.Scan(&count); err {
	case sql.ErrNoRows:
		return false
	case nil:
		break
	default:
		return false
	}
	if count == 0 {
		return false
	} else {
		return true
	}
}

func (db *BlockHeaderDB) HeadersWithoutWitnessOfHeight(height uint64, results chan<- *types.Header) {
	sqlStatement := `SELECT block_data FROM blockheader WHERE block_number = $1 AND dataset_lookup IS NULL`
	rows, err := db.db.Query(sqlStatement, height)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		header := new(types.Header)
		var headerJson string
		err = rows.Scan(&headerJson)
		if err != nil {
			// handle this error
			log.Fatal(err)
		}
		b := []byte(headerJson)
		err = json.Unmarshal(b, &header)
		results <- header
	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		log.Fatal()
	}
	close(results)
}

func (db *BlockHeaderDB) AddWitnessDataForHeader(header *types.Header, dataSetLookup []*big.Int, witnessForLookup []*big.Int) {
	sqlStatement := `UPDATE blockheader SET dataset_lookup = $2, witness_lookup = $3 WHERE block_hash = $1`
	res, err := db.db.Exec(sqlStatement, header.Hash().Hex()[2:], convertToPqArray(dataSetLookup), convertToPqArray(witnessForLookup))
	if err != nil {
		log.Fatal(err)
	}
	_, err = res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
}

func convertToPqArray(pointerArray []*big.Int) interface{} {
	var b []string
	for i := 0; i < len(pointerArray); i++ {
		b = append(b, pointerArray[i].String())
	}
	return pq.Array(b)
}

