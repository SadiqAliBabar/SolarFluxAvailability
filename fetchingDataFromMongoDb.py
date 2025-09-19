from pymongo import MongoClient
import pandas as pd
from datetime import datetime

def flatten_json(json_obj, parent_key='', separator='.'):
    items = []
    for key, value in json_obj.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_json(value, new_key, separator).items())
        elif isinstance(value, list):
            for i, val in enumerate(value):
                if isinstance(val, (dict, list)):
                    items.extend(flatten_json({f"{new_key}[{i}]": val}, '', separator).items())
                else:
                    items.append((f"{new_key}[{i}]", val))
        else:
            items.append((new_key, value))
    return dict(items)


def fetch_level_data(connection_string,
                     level="plant",
                     plant_name=None,
                     start_date=None,
                     end_date=None,
                     save_csv=False,
                     collection_override=None):
    try:
        client = MongoClient(connection_string)

        # Build date filter
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23)

            if level == "string":
                # ✅ String-level collection uses Day_Hour field (format: "YYYY-MM-DD HH")
                query = {
                    "Day_Hour": {
                        "$gte": start_dt.strftime('%Y-%m-%d %H'),
                        "$lte": end_dt.strftime('%Y-%m-%d %H')
                    }
                }
            else:
                # ✅ Plant-level (and others) still use timestamp
                query = {
                    "timestamp": {
                        "$gte": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        "$lte": end_dt.strftime('%Y-%m-%d %H:%M:%S')
                    }
                }
        else:
            query = {}

        # Plant database selection
        if not plant_name or plant_name.upper() == "ALL":
            db_list = [db for db in client.list_database_names() if db.startswith("shams_")]
        else:
            db_list = [f"shams_{plant_name}"]

        all_dataframes = []

        # Default collection mapping
        if collection_override:
            collection_name = collection_override
        elif level == "plant":
            collection_name = "HR_PL_PRD"
        elif level == "string":
            collection_name = "HR_IL_PRD_SADIQ"
        else:
            collection_name = f"{level.upper()}_PRD"

        for db_name in db_list:
            db = client[db_name]

            if collection_name not in db.list_collection_names():
                print(f"⚠️ Collection {collection_name} not found in {db_name}, skipping…")
                continue

            collection = db[collection_name]
            documents = list(collection.find(query))

            if not documents:
                print(f"⚠️ No data in {collection_name} of {db_name} for given range")
                continue

            flattened_data = [flatten_json(doc) | {"Plant": db_name.replace("shams_", "")} for doc in documents]
            df = pd.DataFrame(flattened_data)

            if "_id" in df.columns:
                df = df.drop("_id", axis=1)

            all_dataframes.append(df)

            if save_csv:
                safe_start = start_date.replace("-", "_") if start_date else "ALLTIME"
                safe_end = end_date.replace("-", "_") if end_date else "NOW"
                output_file = f"{collection_name}_{db_name}_{safe_start}_to_{safe_end}.csv"
                df.to_csv(output_file, index=False)
                print(f"✅ Saved {output_file}")

        client.close()

        if all_dataframes:
            return pd.concat(all_dataframes, ignore_index=True)
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"❌ Error in fetch_level_data: {e}")
        return pd.DataFrame()
