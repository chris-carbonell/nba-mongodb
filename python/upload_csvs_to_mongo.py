# general
import glob
import datetime
import os
import sys

# data
import pandas as pd

# mongo
import mongodb_tools as mdt

# config
import config

if __name__ == "__main__":

    # start log
    print(", ".join([datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), "done"]))
    
    # get MongoClient
    client = mdt.get_mongo_client(host = config.host, port = config.port, username = config.username, password = config.password)
    
    # client checks

    if client is None:
        sys.exit("error: client not established")
        
    if client.list_database_names() == []:
        sys.exit("error: no databases found")

    # get CSVs
    ls_csvs = glob.glob(config.dir_data + "*.csv") # get all CSVs

    # loop
    for csv_path in ls_csvs:
        
        # get collection name from filename
        tuple_split = os.path.split(csv_path) # (dir, filename)
        collection_name = tuple_split[1][:-4] # remove .csv

        # get df
        df_csv = pd.read_csv(csv_path, header = 0)

        # upload to MongoDB
        bool_success = mdt.df_to_mongodb(client = client, db_name = config.db_name, collection_name = collection_name, df = df_csv)
        print(", ".join([
            datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
            str(bool_success),
            collection_name
            ]))

    # close log
    print(", ".join([datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), "done"]))