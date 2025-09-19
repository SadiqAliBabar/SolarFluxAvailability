Solar Plant Availability Calculator - User Manual
Overview
This tool calculates daily availability for solar power plants at various granularity levels: Plant, Inverter, MPPT (Maximum Power Point Tracker), and String. Availability is computed using two formulas:

Formula A: Time-based availability (percentage of time the system is operational when irradiance exceeds a threshold).
Formula B: Irradiance-weighted availability (weighted by actual irradiance levels when operational).

The tool fetches historical data from a MongoDB database, processes it with Pandas, performs calculations, and exports results to an Excel file with color-coded availability percentages (e.g., green for 100%, red for <80%).
Key Features

Supports multiple plants (or "all" via auto-discovery).
Date-range filtering for data fetching.
Automatic Excel formatting with conditional coloring.
Debug data generation for troubleshooting.
CLI-driven for easy automation.

Supported Levels



Level
Description
Data Collection Used
Key Metrics



Plant
Overall plant performance
HR_PL_PRD
Inverter power, radiation intensity


Inverter
Per-inverter performance
Plant-level fallback (incomplete)
Inverter power (InverterPower), radiation


MPPT
Per-MPPT performance
Plant-level fallback (incomplete)
MPPT-specific power columns (e.g., mppt_1_power)


String
Per-string performance
HR_IL_PRD_SADIQ (pre-merged with radiation)
Watts per string (Watt/String), radiation


Note: Inverter and MPPT levels use plant-level data as a fallback (not fully implemented for dedicated collections). String level filters for configured strings only (String_Configured == 1).
Prerequisites
Software Requirements

Python: 3.8+ (tested with 3.12).
MongoDB Client: Access to a MongoDB instance (e.g., mongodb://110.39.23.106:27023/).
Libraries: Install via pip:pip install pymongo pandas openpyxl numpy


pymongo: For MongoDB connections.
pandas: Data manipulation.
openpyxl: Excel export and styling.
numpy: Numerical computations.



Hardware/Environment

Stable internet/network access to MongoDB server.
Write permissions for CSV/Excel outputs in the working directory.
Optional: Jupyter/Colab for testing (uncomment if __name__ == "__main__" blocks).

Database Setup

Databases must be named shams_<plant_name> (e.g., shams_coca_cola_faisalabad).
Collections:
Plant: HR_PL_PRD (timestamp-based).
String: HR_IL_PRD_SADIQ (Day_Hour format: "YYYY-MM-DD HH").


Data includes nested JSON fields (flattened automatically).
Radiation intensity (radiation_intensity or dataItemMap.radiation_intensity) must be present.

Installation

Clone or download the repository containing:

main.py (CLI entrypoint).
fetchingDataFromMongoDb.py (Data fetching).
check_availibility.py (Availability calculations).


Place all files in the same directory.

Install dependencies (see above).

Test MongoDB connection:
from pymongo import MongoClient
client = MongoClient("mongodb://your-connection-string:port/")
print(client.list_database_names())  # Should list `shams_*` DBs



Usage
Running the Tool
Execute via command line:
python main.py --level <level> --start_date YYYY-MM-DD --end_date YYYY-MM-DD [optional args]

Required Arguments



Argument
Type
Description
Example



--level
str
Calculation level: plant, inverter, mppt, or string.
--level plant


--start_date
str
Start date (YYYY-MM-DD). Data fetched from this date onward.
--start_date 2023-01-01


--end_date
str
End date (YYYY-MM-DD). Data up to 23:00 on this date.
--end_date 2023-01-31


Optional Arguments



Argument
Type
Default
Description
Example



--plant_name
str
all
Specific plant name or all (auto-discovers from DBs).
--plant_name coca_cola_faisalabad


--connection_string
str
mongodb://110.39.23.106:27023/
MongoDB URI.
--connection_string mongodb://localhost:27017/


--formula
str
A
A (time-based) or B (irradiance-weighted).
--formula B


--irradiance_threshold
float
0.05
Min irradiance (kW/m²) for "operational" conditions.
--irradiance_threshold 0.1


--output_excel
str
combined_availability_YYYYMMDD_HHMMSS.xlsx
Output file path.
--output_excel results.xlsx


Example Commands

All plants, plant-level, Formula A (default):
python main.py --level plant --start_date 2023-01-01 --end_date 2023-01-31


Specific plant, string-level, Formula B:
python main.py --level string --start_date 2023-06-01 --end_date 2023-06-30 --plant_name coca_cola_faisalabad --formula B


Custom MongoDB, higher threshold:
python main.py --level mppt --start_date 2024-09-01 --end_date 2024-09-19 --connection_string mongodb://prod-server:27017/ --irradiance_threshold 0.08 --output_excel custom_mppt.xlsx



Process Flow

CLI Parsing (main.py):

Validate arguments.
If plant_name=all, query MongoDB for all shams_* databases to get plant list.


Data Fetching (fetchingDataFromMongoDb.py):

Connect to MongoDB.
Build query: Date filter on timestamp (plant levels) or Day_Hour (string level).
Select collection based on level (e.g., HR_PL_PRD for plant).
Fetch documents, flatten nested JSON (e.g., dataItemMap.radiation_intensity → dataItemMap.radiation_intensity).
Add Plant column (e.g., from DB name shams_plant → plant).
Optional: Save raw CSV per plant/collection.
Concatenate DataFrames for multi-plant runs.


Availability Calculation (check_availibility.py):

Convert timestamp to datetime.
For String Level: Filter String_Configured == 1; create unique string_id (Plant_sn_MPPT_Strings).
Compute conditions:
Numerator (A/B): Irradiance > threshold AND Power > 0.
Denominator (A): Irradiance > threshold.
Weights (B): Irradiance values where conditions met.


Aggregate by Date (± ID for sub-levels).
Calculate %: (numerator / denominator) * 100 (or weighted sums for B).
Handle edge cases: "Data Unavailable" if denominator=0.


Output Generation:

Per-plant Excel (e.g., plant_daily_A.xlsx) with raw debug data.
Combined Excel: Columns like Date, Plant/ID, Numerator, Denominator/Weights, Availability.
Apply colors: Green (100%), Blue (98-100%), Yellow (95-98%), etc.
Console summary: Records count, avg/range %.


Debug/Intermediates:

Raw CSV: string_raw.csv (string level).
Debug DF: Timestamp, power, radiation, conditions (not saved by default).



Data Flow Diagram (Text-Based)
CLI Args → main.py
          ↓
Plant List (DB Query)
          ↓
For Each Plant:
  Fetch Data (MongoDB Query + Flatten) → input_df
  ↓
  calculate_availability_main → Level-Specific Calc (e.g., calculate_string_availability)
    ↓ (Conditions + Aggregation)
  Daily Availability DF + Debug DF
    ↓
  Per-Plant Excel (Colored)
          ↓
Combined Excel (All Plants)

Expected Output
Console Output
🚀 Starting availability calculation for PLANT level...
📅 Date range: 2023-01-01 to 2023-01-31
🏭 Plant(s): all
📈 Formula: A

🌟 Processing plant: Coca Cola Faisalabad (DB: shams_coca_cola_faisalabad)
📊 Calculating availability for PLANT level for Coca Cola Faisalabad...
🏭 Processing Plant: Coca Cola Faisalabad | Formula: A
📈 Results: 31 records | Avg: 99.2% | Range: 95.5%-100.0%

✅ Combined availability completed. Shape=(31, 5)
📈 Results saved to: combined_availability_20230919_143022.xlsx

Excel Output

Filename: combined_availability_YYYYMMDD_HHMMSS.xlsx (or custom).

Sheet: Single sheet with columns:



Column
Description
Formula A
Formula B



Date
YYYY-MM-DD
Yes
Yes


Plant/ID (e.g., sn_id, string_id)
Identifier
Yes
Yes


Condition_Numerator/Actual_Weight
Operational periods/sum
Yes
- / Yes


Condition_Denominator/Potential_Weight
Total potential periods/sum
Yes
- / Yes


Availability
% (colored)
Yes
Yes



Color Coding (Availability column):



Range
Color
Hex



100%
Green
00FF00


98-99.99%
Blue
0000FF


95-97.99%
Yellow
FFFF00


80-94.99%
Orange
FFA500


<80%
Red
FF0000


N/A
Gray
808080



Per-Plant Files: Generated in working dir (e.g., HR_PL_PRD_shams_coca_cola_faisalabad_2023_01_01_to_2023_01_31.csv for raw; Excel for results).


Sample Data (Formula A, Plant Level)



Date
Plant
Condition_Numerator
Condition_Denominator
Availability



2023-01-01
Coca Cola Faisalabad
12
12
100.00


2023-01-02
Coca Cola Faisalabad
11
12
91.67


Troubleshooting
Common Errors



Error
Cause
Fix



Collection not found
Wrong level/collection.
Verify DB schema; use --level string for HR_IL_PRD_SADIQ.


No data available
Empty query results.
Check date range, plant name; extend range or use --plant_name all.


KeyError: 'radiation_intensity'
Missing field in data.
Ensure DB has radiation data; flatten_json handles nesting.


Invalid formula
Typo in --formula.
Use A or B only.


Connection failed
MongoDB URI issue.
Update --connection_string; test with pymongo.


Excel save error
Permissions.
Run in writable dir; check openpyxl install.


Debugging Tips

Set save_csv=True in fetch_level_data (edit code) for raw CSVs.
Uncomment if __name__ == "__main__" in check_availibility.py for standalone tests:df = pd.read_csv('string_raw.csv')
results, debug = calculate_availability_main(df, 'Test Plant', 'string', 'A')
print(results.head())


Logs: Check console for ⚠️ warnings (e.g., no configured strings).
Performance: For large dates (>1 month), limit plants or use aggregation in MongoDB queries.

Customization

Add levels: Extend calculate_availability_main dispatcher.
Thresholds: Modify irradiance_threshold arg.
Outputs: Edit to_excel calls for multi-sheet or charts.

Support

Report issues: [GitHub Repo Link].
Questions: Review console output or debug CSVs.

Last Updated: September 19, 2025
