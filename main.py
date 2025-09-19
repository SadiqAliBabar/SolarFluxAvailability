import argparse
import pandas as pd
from pymongo import MongoClient

from fetchingDataFromMongoDb import fetch_level_data
from check_availibility import calculate_availability_main


def get_plant_names(connection_string):
    """Retrieve list of plant names from MongoDB databases starting with 'shams_'."""
    try:
        client = MongoClient(connection_string)
        db_names = [db for db in client.list_database_names() if db.startswith("shams_")]
        plant_names = [db.replace("shams_", "") for db in db_names]
        client.close()
        return plant_names
    except Exception as e:
        print(f"❌ Error retrieving plant names: {str(e)}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Calculate solar plant availability")
    parser.add_argument('--level', type=str, required=True,
                        choices=['plant', 'inverter', 'mppt', 'string'],
                        help="Level for availability calculation")
    parser.add_argument('--start_date', type=str, required=True,
                        help="Start date in YYYY-MM-DD format")
    parser.add_argument('--end_date', type=str, required=True,
                        help="End date in YYYY-MM-DD format")
    parser.add_argument('--plant_name', type=str, default="all",
                        help="Plant name or 'all' (default: all)")
    parser.add_argument('--connection_string', type=str,
                        default="mongodb://110.39.23.106:27023/",
                        help="MongoDB connection string")
    parser.add_argument('--formula', type=str, default="A", choices=["A", "B"],
                        help="Formula: A (time-based) or B (irradiance-weighted)")
    parser.add_argument('--irradiance_threshold', type=float, default=0.05,
                        help="Irradiance threshold")
    parser.add_argument('--output_excel', type=str,
                        default=f"combined_availability_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        help="Output Excel file")

    args = parser.parse_args()

    print(f"🚀 Starting availability calculation for {args.level.upper()} level...")
    print(f"📅 Date range: {args.start_date} to {args.end_date}")
    print(f"🏭 Plant(s): {args.plant_name}")
    print(f"📈 Formula: {args.formula}")

    # Get list of plants
    if args.plant_name.lower() == "all":
        plant_names = get_plant_names(args.connection_string)
        if not plant_names:
            print("❌ No plants found in MongoDB. Exiting.")
            return None
    else:
        plant_names = [args.plant_name]

    all_availability_dfs = []

    for plant_name_db in plant_names:
        plant_name_display = plant_name_db.replace('_', ' ')
        print(f"\n🌟 Processing plant: {plant_name_display} (DB: shams_{plant_name_db})")

        try:
            if args.level == "plant":
                # Fetch plant-level data
                df_plant = fetch_level_data(
                    connection_string=args.connection_string,
                    level="plant",
                    plant_name=plant_name_db,
                    start_date=args.start_date,
                    end_date=args.end_date
                )
                input_df = df_plant

            elif args.level == "string":
                # Fetch string-level data directly (already has radiation_intensity)
                df_string = fetch_level_data(
                    connection_string=args.connection_string,
                    level="string",
                    plant_name=plant_name_db,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    collection_override="HR_IL_PRD_SADIQ"  # ⚡ Using pre-merged collection
                )
                # ✅ Skip process_string_level_data, use df_string directly
                input_df = df_string  # or df_string depending on what you're feeding

                # 🔧 Convert key numeric columns
                numeric_cols = ["Watt/String", "radiation_intensity", "InverterPower", "StringCapacity"]
                for col in numeric_cols:
                    if col in input_df.columns:
                        input_df[col] = pd.to_numeric(input_df[col], errors="coerce")

                df_string.to_csv("string_raw.csv", index=False)

                if df_string.empty:
                    print(f"❌ No string-level data for {plant_name_display}")
                    continue

                input_df = df_string

            elif args.level in ["inverter", "mppt"]:
                print(f"⚠️ Inverter/MPPT not fully implemented, using plant-level fallback")
                df_plant = fetch_level_data(
                    connection_string=args.connection_string,
                    level="plant",
                    plant_name=plant_name_db,
                    start_date=args.start_date,
                    end_date=args.end_date
                )
                input_df = df_plant

            else:
                print(f"❌ Unsupported level: {args.level}")
                continue

            # Step 4: Calculate availability
            if input_df is not None and not input_df.empty:
                print(f"📊 Calculating availability for {args.level.upper()} level for {plant_name_display}...")
                daily_availability, debug_df = calculate_availability_main(
                    df=input_df,
                    plant_name=plant_name_display,
                    level=args.level,
                    formula=args.formula,
                    irradiance_threshold=args.irradiance_threshold
                )

                if 'Plant' not in daily_availability.columns:
                    daily_availability['Plant'] = plant_name_display

                all_availability_dfs.append(daily_availability)

        except Exception as e:
            print(f"❌ Error processing {plant_name_display}: {e}")
            import traceback; traceback.print_exc()
            continue

    # Step 5: Save results
    if all_availability_dfs:
        final_availability_df = pd.concat(all_availability_dfs, ignore_index=True)
        print(f"\n✅ Combined availability completed. Shape={final_availability_df.shape}")
        final_availability_df.to_excel(args.output_excel, index=False)
        print(f"📈 Results saved to: {args.output_excel}")
        return final_availability_df
    else:
        print("\n❌ No availability results generated.")
        return None


if __name__ == "__main__":
    final_df = main()
    if final_df is not None:
        print("\n📋 Sample output:")
        print(final_df.head())
