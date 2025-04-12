
# 📊 IoT Sensor Data Analysis using PySpark

This project demonstrates real-time sensor data analysis using Apache Spark (PySpark). The dataset contains simulated IoT sensor readings (e.g., temperature, humidity, timestamps, and locations). The analysis includes filtering, aggregation, time-based patterns, pivoting, and window functions.

---

## 📁 Project Structure

```
.
├── sensor_data.csv                  # Input dataset containing IoT sensor readings
├── data_generator.py               # Script to generate synthetic sensor data using Faker
├── task1_basic_exploration.py      # Basic exploration: show data, distinct locations, record count
├── task2_filter_aggregate.py       # Filters in-range/out-of-range temps & aggregates by location
├── task3_time_analysis.py          # Analyzes temperature by hour of day
├── task4_window_function.py        # Ranks sensors by average temperature using window functions
├── task5_pivot_interpretation.py   # Creates a pivot table of hourly temps per location
├── task1_output.csv/               # Output folder for Task 1 results
├── task2_output.csv/               # Output folder for Task 2 results
├── task3_output.csv/               # Output folder for Task 3 results
├── task4_output.csv/               # Output folder for Task 4 results
├── task5_output.csv/               # Output folder for Task 5 results
```

---

## 🚀 Tasks Summary

| Task | Description |
|------|-------------|
| **Task 1** | Basic Data Exploration – Counts, distinct locations, and preview |
| **Task 2** | Filters temperatures within safe range and computes average by location |
| **Task 3** | Extracts hour from timestamp and computes average temperature per hour |
| **Task 4** | Ranks sensors by average temperature using window functions |
| **Task 5** | Creates pivot table showing hourly average temperatures by location |

---

## 🛠️ Requirements

- Python 3.8+
- PySpark

### 🔧 Install dependencies
```bash
pip install pyspark faker
```

---

## ▶️ How to Run

1. Generate data (if needed):
```bash
python data_generator.py
```

2. Run analysis scripts:
```bash
python task1_basic_exploration.py
python task2_filter_aggregate.py
python task3_time_analysis.py
python task4_window_function.py
python task5_pivot_interpretation.py
```

3. Check output in `task*_output.csv/` folders (each contains a CSV file).

---

## 📬 Output Format

All outputs are saved as CSV folders with headers and overwrite enabled. Use `.coalesce(1)` in the scripts to ensure single file output if needed.


✅ Task 1 – Basic Exploration
```bash
sensor_id,timestamp,temperature,humidity,location,sensor_type
1090,2025-04-08T19:11:10.000Z,34.87,45.56,BuildingB_Floor1,TypeB
1012,2025-04-09T17:18:52.000Z,16.91,30.0,BuildingB_Floor2,TypeB
1045,2025-04-08T00:05:38.000Z,19.14,65.03,BuildingA_Floor2,TypeA
1069,2025-04-08T01:10:56.000Z,27.82,57.78,BuildingA_Floor2,TypeC
1075,2025-04-09T22:12:17.000Z,31.62,54.89,BuildingA_Floor1,TypeC
```

✅ Task 2 – In-range and Out-of-range Counts + Aggregation
Output CSV (task2_output.csv):
```bash
location,avg_temperature,avg_humidity
BuildingB_Floor2,25.02093220338983,53.24694915254236
BuildingA_Floor1,24.73271317829457,56.42717054263564
BuildingA_Floor2,24.65726235741444,56.20019011406841
BuildingB_Floor1,24.539094650205758,55.14160493827159
```

✅ Task 3 – Hourly Temperature Averages
Output CSV (task3_output.csv):
```bash
hour_of_day,avg_temp
0,25.78425531914894
1,25.805161290322577
2,24.20945945945946
3,25.38513513513513
4,24.85818181818182
5,24.482325581395347
```
✅ Task 4 – Top 5 Sensors by Average Temperature
Output CSV (task4_output.csv):
```bash
sensor_id,avg_temp,rank_temp
1056,32.65333333333333,1
1098,29.458750000000002,2
1009,29.14428571428571,3
1064,29.077999999999996,4
1073,28.935454545454547,5
```

✅ Task 5 – Pivot Table (Location × Hour)
Output CSV (task5_output.csv):
```bash
location,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
BuildingA_Floor1,25.07,25.83,22.3,26.99,22.82,25.58,24.88,27.2,26.17,24.3,23.98,24.69,22.98,25.36,25.17,22.9,25.26,25.05,24.22,21.48,28.27,24.34,25.8,23.32
BuildingB_Floor2,24.47,25.35,25.08,25.1,27.13,25.01,26.79,22.44,27.54,27.57,22.79,28.16,22.96,23.19,25.24,25.65,17.88,26.9,25.66,25.83,23.24,22.71,25.33,27.46
BuildingA_Floor2,26.6,22.45,27.53,25.68,24.76,25.72,25.84,20.85,22.41,22.64,24.51,25.33,25.92,21.33,25.72,24.67,25.93,24.18,23.76,26.83,23.86,27.64,23.44,22.34
BuildingB_Floor1,26.66,27.51,21.56,24.0,25.13,21.62,25.25,22.83,25.6,24.1,24.73,26.74,25.53,25.32,22.47,25.69,24.79,21.97,20.77,27.63,22.33,25.32,23.28,28.0

```

## 📌 Notes

- All scripts use inferred schema and are compatible with moderately large CSV files.
- Uses Spark SQL functions like `avg`, `hour`, `dense_rank`, `pivot`, and `window` operations.

---

