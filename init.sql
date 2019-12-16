CREATE TABLE blockheader (
    block_hash varchar(64) primary key,
    block_number bigint,
    block_data jsonb,
    dataset_lookup bigint[],
    witness_lookup bigint[]
);
