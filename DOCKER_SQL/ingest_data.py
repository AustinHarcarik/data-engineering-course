import argparse
import os
import pandas as pd
from sqlalchemy import create_engine

# function to split dataframe into chunks (for batch sql insertion)
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def main(params):
    # parse params
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    file_name = params.file_name

    # download the parquet
    os.system(f'wget {url} -O {file_name}')

    # read in data
    if 'parquet' in file_name: 
        data = pd.read_parquet(file_name)
    else: 
        data = pd.read_csv(file_name)
    print(f'number of rows read in from file:{len(data)}')

    # create connection engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # show schema
    print(pd.io.sql.get_schema(data, name = table_name, con = engine))

    # create table with schema
    data.head(n=0).to_sql(name = table_name, con = engine, if_exists = 'replace')

    # upload data
    for chunk in chunker(data, 10000):
        chunk.to_sql(name = table_name, con = engine, if_exists = 'append')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'ingest parquet data to postgres')

    parser.add_argument('--user', help = 'user name for postgres')
    parser.add_argument('--password', help = 'password for postgres')
    parser.add_argument('--host', help = 'host for postgres')
    parser.add_argument('--port', help = 'port for postgres')
    parser.add_argument('--db', help = 'database name for postgres')
    parser.add_argument('--table_name', help = 'name of the table to write results to')
    parser.add_argument('--url', help = 'url of the file to ingest')
    parser.add_argument('--file_name', help = 'name of the file to write to')

    args = parser.parse_args()

    main(args)
