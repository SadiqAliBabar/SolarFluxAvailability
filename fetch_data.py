import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def flatten_json(data, parent_key='', sep='.'):
    """Flatten nested JSON/dict, handling lists and dicts."""
    items = {}
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                items.update(flatten_json({f"{new_key}[{i}]": item}, '', sep))
        else:
            items[new_key] = value
    return items

def get_plant_names(connection_string):
    """Retrieve filtered list of plant names from 'shams_*' DBs, excluding invalid ones."""
    try:
        client = MongoClient(connection_string)
        dbs = client.list_database_names()
        valid_dbs = [db for db in dbs if db.startswith('shams_') and db not in ['shams_admin', 'shams_']]
        plants = [db.replace('shams_', '') for db in valid_dbs]
        client.close()
        return plants
    except Exception as e:
        logging.error(f"Error retrieving plant names: {e}")
        return []

def fetch_plant_data(connection_string, plant_name=None, start_date=None, end_date=None):
    """
    Fetch data for plant level.
    - Uses DBs: shams_{plant_name} (or all valid shams_* if 'all').
    - Collection: HR_PL_PRD
    - Query filter: timestamp if dates provided.
    """
    try:
        client = MongoClient(connection_string)
        query = {}
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            query['timestamp'] = {
                '$gte': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                '$lte': end_dt.strftime('%Y-%m-%d %H:%M:%S')
            }

        dataframes = []

        if plant_name is None or (isinstance(plant_name, str) and plant_name.lower() == 'all'):
            db_list = [f"shams_{p}" for p in get_plant_names(connection_string)]
        elif isinstance(plant_name, list):
            db_list = [f"shams_{p}" for p in plant_name]
        else:
            db_list = [f"shams_{plant_name}"]

        collection_name = 'HR_PL_PRD'

        for db_name in db_list:
            db = client[db_name]
            if collection_name not in db.list_collection_names():
                logging.warning(f"Collection {collection_name} not found in {db_name}.")
                continue
            docs = list(db[collection_name].find(query))
            if not docs:
                logging.warning(f"No data in {collection_name} of {db_name}.")
                continue
            flattened = [flatten_json(doc) | {'Plant': db_name.replace('shams_', '')} for doc in docs]
            df = pd.DataFrame(flattened).drop('_id', axis=1, errors='ignore')
            dataframes.append(df)

        client.close()
        if dataframes:
            return pd.concat(dataframes, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error fetching plant data: {e}")
        return pd.DataFrame()

def fetch_inverter_data(connection_string, plant_name=None, inverter_sn=None, start_date=None, end_date=None):
    """
    Fetch data for inverter level.
    - Uses DB: shams_admin
    - Collection: alerter2
    - Query filter: timestamp if dates provided, plus Plant and sn.
    """
    try:
        client = MongoClient(connection_string)
        query = {}
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            query['timestamp'] = {
                '$gte': start_dt,
                '$lte': end_dt
            }

        db_name = 'shams_admin'
        db = client[db_name]
        collection_name = 'ALL_HR_ILMP_PRD_AVAIL'
        if collection_name not in db.list_collection_names():
            logging.warning(f"Collection {collection_name} not found in {db_name}.")
            return pd.DataFrame()

        if plant_name:
            query['Plant'] = plant_name.replace('_', ' ')
        if inverter_sn and inverter_sn.lower() != 'all':
            if ',' in inverter_sn:
                query['sn'] = {'$in': [sn.strip() for sn in inverter_sn.split(',')]}
            else:
                query['sn'] = inverter_sn

        docs = list(db[collection_name].find(query))
        if not docs:
            logging.warning(f"No inverter data found for {plant_name} ({inverter_sn}).")
            return pd.DataFrame()

        flattened = [flatten_json(doc) for doc in docs]
        df = pd.DataFrame(flattened).drop('_id', axis=1, errors='ignore')
        client.close()
        return df
    except Exception as e:
        logging.error(f"Error fetching inverter data: {e}")
        return pd.DataFrame()

def fetch_mppt_data(connection_string, plant_name=None, inverter_sn=None, mppt_id=None, start_date=None, end_date=None):
    """
    Fetch data for MPPT level.
    - Uses DB: shams_admin
    - Collection: alerter2
    - Unwinds mppts array to get per-MPPT data.
    - Query filter: timestamp, Plant, sn, and optionally mpptId.
    """
    try:
        client = MongoClient(connection_string)
        query = {}
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            query['timestamp'] = {
                '$gte': start_dt,
                '$lte': end_dt
            }

        db_name = 'shams_admin'
        db = client[db_name]
        collection_name = 'ALL_HR_ILMP_PRD_AVAIL'
        if collection_name not in db.list_collection_names():
            logging.warning(f"Collection {collection_name} not found in {db_name}.")
            return pd.DataFrame()

        if plant_name:
            query['Plant'] = plant_name.replace('_', ' ')
        if inverter_sn and inverter_sn.lower() != 'all':
            if ',' in inverter_sn:
                query['sn'] = {'$in': [sn.strip() for sn in inverter_sn.split(',')]}
            else:
                query['sn'] = inverter_sn
        if mppt_id and mppt_id.lower() != 'all':
            if ',' in mppt_id:
                query['mppts.mpptId'] = {'$in': [mid.strip() for mid in mppt_id.split(',')]}
            else:
                query['mppts.mpptId'] = mppt_id

        pipeline = [
            {'$match': query},
            {'$unwind': '$mppts'},
            {'$project': {
                '_id': 0,
                'Plant': 1,
                'sn': 1,
                'timestamp': 1,
                'InverterCapacity': 1,
                'InverterPower': 1,
                'mpptId': '$mppts.mpptId',
                'mppt_Power': '$mppts.mppt_Power',
                'mppt_Capacity': '$mppts.mppt_Capacity',
                'radiation_intensity': 1
            }}
        ]

        docs = list(db[collection_name].aggregate(pipeline))
        if not docs:
            logging.warning(f"No MPPT data found for {plant_name} (sn: {inverter_sn}, mppt: {mppt_id}).")
            return pd.DataFrame()

        df = pd.DataFrame(docs)
        client.close()
        return df
    except Exception as e:
        logging.error(f"Error fetching MPPT data: {e}")
        return pd.DataFrame()

def fetch_string_data(connection_string, plant_name=None, inverter_sn=None, mppt_id=None, string_id=None, start_date=None, end_date=None):
    """
    Fetch data for string level.
    - Uses DBs: shams_{plant_name} (or all valid shams_* if 'all').
    - Collection: HR_IL_PRD_IN
    - Query filter: Day_Hour, Plant, sn, MPPT, and Strings if provided.
    """
    try:
        client = MongoClient(connection_string)
        query = {}
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            query['Day_Hour'] = {
                '$gte': start_dt.strftime('%Y-%m-%d %H'),
                '$lte': end_dt.strftime('%Y-%m-%d %H')
            }

        dataframes = []

        if plant_name is None or (isinstance(plant_name, str) and plant_name.lower() == 'all'):
            db_list = [f"shams_{p}" for p in get_plant_names(connection_string)]
        elif isinstance(plant_name, list):
            db_list = [f"shams_{p}" for p in plant_name]
        else:
            db_list = [f"shams_{plant_name}"]

        collection_name = 'HR_IL_PRD_IN'

        for db_name in db_list:
            db = client[db_name]
            if collection_name not in db.list_collection_names():
                logging.warning(f"Collection {collection_name} not found in {db_name}.")
                continue
            plant_display = db_name.replace('shams_', '')
            query_copy = query.copy()
            if inverter_sn and inverter_sn.lower() != 'all':
                if ',' in inverter_sn:
                    query_copy['sn'] = {'$in': [sn.strip() for sn in inverter_sn.split(',')]}
                else:
                    query_copy['sn'] = inverter_sn
            if mppt_id and mppt_id.lower() != 'all':
                if ',' in mppt_id:
                    query_copy['MPPT'] = {'$in': [mid.strip() for mid in mppt_id.split(',')]}
                else:
                    query_copy['MPPT'] = mppt_id
            if string_id and string_id.lower() != 'all':
                if ',' in string_id:
                    query_copy['Strings'] = {'$in': [sid.strip() for sid in string_id.split(',')]}
                else:
                    query_copy['Strings'] = string_id

            docs = list(db[collection_name].find(query_copy))
            if not docs:
                logging.warning(f"No data in {collection_name} of {db_name} for query: {query_copy}.")
                continue
            df = pd.DataFrame(docs).drop('_id', axis=1, errors='ignore')
            df['Plant'] = plant_display
            dataframes.append(df)

        client.close()
        if dataframes:
            return pd.concat(dataframes, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error fetching string data: {e}")
        return pd.DataFrame()

if __name__ == '__main__':
    # Test CLI: python fetch_data.py --level plant --plant_name all --start_date 2025-01-01 --end_date 2025-01-10
    import argparse
    parser = argparse.ArgumentParser(description="Fetch MongoDB data by level.")
    parser.add_argument('--level', required=True, choices=['plant', 'inverter', 'mppt', 'string'])
    parser.add_argument('--plant_name', default='all')
    parser.add_argument('--inverter_sn', default='all')
    parser.add_argument('--mppt_id', default='all')
    parser.add_argument('--string_id', default='all')
    parser.add_argument('--start_date')
    parser.add_argument('--end_date')
    parser.add_argument('--connection_string', default='mongodb://110.39.23.106:27023/')
    args = parser.parse_args()

    if args.level == 'plant':
        df = fetch_plant_data(args.connection_string, args.plant_name, args.start_date, args.end_date)
    elif args.level == 'inverter':
        df = fetch_inverter_data(args.connection_string, args.plant_name, args.inverter_sn, args.start_date, args.end_date)
    elif args.level == 'mppt':
        df = fetch_mppt_data(args.connection_string, args.plant_name, args.inverter_sn, args.mppt_id, args.start_date, args.end_date)
    elif args.level == 'string':
        df = fetch_string_data(args.connection_string, args.plant_name, args.inverter_sn, args.mppt_id, args.string_id, args.start_date, args.end_date)
    print(df.head() if not df.empty else "No data fetched.")