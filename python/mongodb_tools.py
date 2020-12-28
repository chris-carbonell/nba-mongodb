# data
import pandas as pd
import json

# db
from pymongo import MongoClient, errors

def get_mongo_client(host, port, username, password):
    '''
    get mongo client

    args:
    host (str): IP address of Socket Address (e.g., 127.0.0.1 of 127.0.0.1:8888)
    port (str): port of Socket Address (e.g., 8888 of 127.0.0.1:8888)
    username (str): mongo root username (see ./secrets/mongo.env)
    password (str): mongo root password (see ./secrets/mongo.env)

    returns:
    MongoClient (obj): mongo client
    '''
    
    # https://kb.objectrocket.com/mongo-db/use-docker-and-python-for-a-mongodb-application-1046
    
    # use a try-except indentation to catch MongoClient() errors
    try:
        # try to instantiate a client instance
        client = MongoClient(
            host = [ str(host) + ":" + str(port) ],
            serverSelectionTimeoutMS = 3000, # 3 second timeout
            username = username, # MONGO_INITDB_ROOT_USERNAME
            password = password, # MONGO_INITDB_ROOT_PASSWORD
        )
        
    except errors.ServerSelectionTimeoutError as err:
        # set the client and DB name list to 'None' and `[]` if exception
        client = None
        
    return client

def df_from_mongodb(client, db_name, collection_name, bool_id = False):
    '''
    download collection to dataframe

    args:
    client (obj): MongoClient
    db_name (str): name of db (e.g., "nba_data")
    collection_name (str): name of collection (e.g., "commonallplayers")
    bool_id (bool): if True, drop the useless _id column

    returns:
    df_cursor (pd.DataFrame): pandas dataframe
    '''
    
    # get collection
    db = client[db_name]
    cursor = db[collection_name].find({})
    
    # convert to dataframe
    df_cursor = pd.DataFrame(list(cursor))

    # cleanup
    if not bool_id:
        if "_id" in df_cursor.columns:
            df_cursor.drop("_id", axis = 1, inplace = True)
    
    return df_cursor

def df_to_mongodb(client, db_name, collection_name, df):
    '''
    upload dataframe to collection

    args:
    client (obj): MongoClient
    db_name (str): name of db (e.g., "nba_data")
    collection_name (str): name of collection (e.g., "commonallplayers")
    df (pd.DataFrame): pandas dataframe

    returns:
    None
    '''

    try:
        # database & collection
        db = client[db_name] # database
        Collection = db[collection_name] # collection (ie table)

        # transpose
        records = json.loads(df.T.to_json()).values()

        # insert
        Collection.insert_many(records)

        return True

    except:
        return False
    
    return

def df_to_mongodb(client, db_name, collection_name, df):
    '''
    upload dataframe to collection

    args:
    client (obj): MongoClient
    db_name (str): name of db (e.g., "nba_data")
    collection_name (str): name of collection (e.g., "commonallplayers")
    df (pd.DataFrame): pandas dataframe

    returns:
    None
    '''

    try:
        # database & collection
        db = client[db_name] # database
        Collection = db[collection_name] # collection (ie table)

        # transpose
        records = json.loads(df.T.to_json()).values()

        # insert
        Collection.insert_many(records)

        return True

    except:
        return False
    
    return