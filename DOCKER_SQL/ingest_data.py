import argparse
import os
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    # parse params
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'

    # download the parquet
    os.system(f'wget {url} -O {parquet_name}')

    # read in data
    data = pd.read_parquet(parquet_name)
    print(f'number of rows read in from parquet file:{len(data)}')

    # create connection engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # show schema
    print(pd.io.sql.get_schema(data, name = table_name, con = engine))

    # create table with schema
    data.head(n=0).to_sql(name = table_name, con = engine, if_exists = 'replace')

    # upload data
    data.head(n=100).to_sql(name = table_name, con = engine, if_exists = 'append')

    # example of how to query the data from python
    # query = "select * from yellow_taxi_data limit 10;"
    # pd.read_sql(query, con = engine)

if __name__ == '__main__':
    # instantiate parser
    parser = argparse.ArgumentParser(description = 'ingest parquet data to postgres')

    parser.add_argument('--user', help = 'user name for postgres')
    parser.add_argument('--password', help = 'password for postgres')
    parser.add_argument('--host', help = 'host for postgres')
    parser.add_argument('--port', help = 'port for postgres')
    parser.add_argument('--db', help = 'database name for postgres')
    parser.add_argument('--table_name', help = 'name of the table to write results to')
    parser.add_argument('--url', help = 'url of the parquet file')

    args = parser.parse_args()

    main(args)
