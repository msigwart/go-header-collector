# Header Collector
This project consists of small command line utilities to collect block headers from an Ethereum node.
The block headers are collected via websockets and stored in a PostgreSQL database.

## Prerequisites
### Run Ethereum node with Docker
Start an Ethereum light client (Geth) with Docker:
```
docker run --name geth-node -P -v /tmp/geth:/root/.ethereum ethereum/client-go --syncmode light --rpc --rpcaddr 0.0.0.0 --ws --wsaddr 0.0.0.0
``` 

The websocket connection will be available under `ws://localhost:8546`.

---
Alternatively, you can connect to Infura via `"wss://mainnet.infura.io/ws/v3/<PROJECT_ID>"`

### Setup Postgres database with Docker
1. Start Postgres DB: 
    ```
    docker run -P -d --name blockheader-db -e POSTGRES_DB=blockheader postgres
    ```
2. Copy init script
    ```
    docker cp ./init.sql blockheader-db:/
    ```
3. Init database
    ```
   docker exec -it blockheader-db psql -d blockheader -U postgres -f /init.sql
    ```
4. Connect to database
    ```
   docker exec -it blockheader-db psql -d blockheader -U postgres
    ```

## Get Started
After cloning the repository, install the binaries with
```go install ./...```.

This installs the following executable binaries:
* `header-collector`
* `dag-generator`
* `witness-generator`

### `header-collector`
This command starts the header collection process. 
For that, it needs to connect to a running Ethereum node via Websockets. 
The command subscribes to all block headers from the node and stores them in the Postgres database.

### `dag-generator`
This command generates the cache files which are needed for the witness data generation. 
This should be run with the corresponding parameters before executing command `witness-generator`.

### `witness-generator`
This command generates the witness data for each block header that was collected by command `header-collector`.
The data is added to the corresponding block header entry in the Postgres database. 
