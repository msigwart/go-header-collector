package go_header_collector

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"log"
	"math/big"
)

type BlockHeaderDB struct {
	db *sql.DB
}

func (db *BlockHeaderDB) InsertBlockHeader(header *types.Header) (sql.Result, error) {
	headerJson, err := json.Marshal(header)
	if err != nil {
		log.Fatal(err)
	}

	sqlStatement := `
INSERT INTO blockheader (block_hash, block_number, block_data)
VALUES ($1, $2, $3)`
	return db.db.Exec(sqlStatement, header.Hash().Hex()[2:], header.Number.String(), string(headerJson))
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

func (db *BlockHeaderDB) MinBlockNumber() (uint64, error) {
	sqlStatement := `SELECT MIN(block_number) FROM blockheader`
	var minBlockNumber uint64
	row := db.db.QueryRow(sqlStatement)
	switch err := row.Scan(&minBlockNumber); err {
	case sql.ErrNoRows:
		fmt.Println()
		return 0, fmt.Errorf("no rows without witness data exist")
	case nil:
		break
	default:
		return 0, err
	}
	return minBlockNumber, nil
}

func (db *BlockHeaderDB) MaxBlockNumber() (uint64, error) {
	sqlStatement := `SELECT MAX(block_number) FROM blockheader`
	var maxBlockNumber uint64
	row := db.db.QueryRow(sqlStatement)
	switch err := row.Scan(&maxBlockNumber); err {
	case sql.ErrNoRows:
		return 0, fmt.Errorf("no rows without witness data exist")
	case nil:
		break
	default:
		return 0, err
	}
	return maxBlockNumber, nil
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

func (db *BlockHeaderDB) HeadersOfHeight(height uint64, results chan<- *types.Header) {
	sqlStatement := `SELECT block_data FROM blockheader WHERE block_number = $1`
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
	close(results)
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		log.Fatal()
	}
}

func (db *BlockHeaderDB) BlocksWithMissingParents(from uint64, to uint64, results chan<- *types.Header) {
	sqlStatement := `
select b.block_data
from (select * from blockheader where block_number > $1 and block_number <= $2) as h
right join (select * from blockheader where block_number > $1 and block_number <= $2) as b
on b.block_data ->> 'parentHash' = h.block_data ->> 'hash' 
where h.block_data is null;
`
	rows, err := db.db.Query(sqlStatement, from, to)
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
	close(results)
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		log.Fatal()
	}
}

func (db *BlockHeaderDB) MoveToOrphans(hash common.Hash) {
	sqlStatement := `
WITH moved_rows AS (
    DELETE FROM blockheader b
    WHERE b.block_data ->> 'hash' = $1
    RETURNING b.*
)
INSERT INTO orphan --specify columns if necessary
SELECT DISTINCT * FROM moved_rows;
`
	_, err := db.db.Exec(sqlStatement, hash.Hex())
	if err != nil {
		log.Fatal(err)
	}
}

func (db *BlockHeaderDB) HasHeaderOfHash(hash common.Hash) (bool, error) {
	sqlStatement := `SELECT COUNT(1) FROM blockheader WHERE block_data ->> 'hash' = $1`
	row := db.db.QueryRow(sqlStatement, hash.Hex())
	var count uint
	switch err := row.Scan(&count); err {
	case sql.ErrNoRows:
		return false, err
	case nil:
		break
	default:
		return false, err
	}
	if count == 0 {
		return false, nil
	} else {
		return true, nil
	}
}


func (db *BlockHeaderDB) HeadersWithoutWitness(start uint64, end uint64, results chan<- *types.Header) {
	var rows *sql.Rows
	var err error
	sqlStatement := `SELECT block_data FROM blockheader WHERE block_number >= $1 AND block_number < $2 AND dataset_lookup IS NULL ORDER BY block_number`
	for {
		rows, err = db.db.Query(sqlStatement, start, end)
		if err == nil {
			break
		}
		log.Println(err)
		log.Println("Trying again ...")
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

func (db *BlockHeaderDB) AddWitnessDataForHeader(header *types.Header, dataSetLookup []*big.Int, witnessForLookup []*big.Int) (int64, error)  {
	sqlStatement := `UPDATE blockheader SET dataset_lookup = $2, witness_lookup = $3 WHERE block_hash = $1`
	res, err := db.db.Exec(sqlStatement, header.Hash().Hex()[2:], convertToPqArray(dataSetLookup), convertToPqArray(witnessForLookup))
	if err != nil {
		log.Fatal(err)
	}
	return res.RowsAffected()
}

func convertToPqArray(pointerArray []*big.Int) interface{} {
	var b []string
	for i := 0; i < len(pointerArray); i++ {
		b = append(b, pointerArray[i].String())
	}
	return pq.Array(b)
}

