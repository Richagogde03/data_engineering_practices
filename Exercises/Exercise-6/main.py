from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import zipfile
import glob
import os


def load_data():
    """
    Loads all CSV files from ZIP archives in the data folder into a list of Spark DataFrames.
    """
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    base_dir = os.getcwd()
    zip_folder = os.path.join(base_dir, "data", "*.zip")
    report_dir = os.path.join(base_dir, "reports")

    os.makedirs(report_dir, exist_ok=True)
    zip_files = glob.glob(zip_folder)

    all_dfs = []

    for zip_path in zip_files:
        with zipfile.ZipFile(zip_path, 'r') as z:
            for file_name in z.namelist():

                # skip Mac system files
                if file_name.startswith("__MACOSX") or file_name.startswith("._"):
                    continue

                if file_name.endswith(".csv"):
                    print(f"Reading {file_name} from {zip_path}")
                    with z.open(file_name) as f:
                        data = f.read().decode("utf-8")
                        rdd = spark.sparkContext.parallelize(data.splitlines())
                        df = spark.read.csv(rdd, header=True, inferSchema=True)
                        all_dfs.append(df)

    print(f"\nLoaded {len(all_dfs)} CSV files from ZIPs.")
    return all_dfs


def main():
    all_dfs = load_data()
    df_2019_Q4 = all_dfs[0]
    df_2020_Q1 = all_dfs[1]

    # print("First ZIP DataFrame:")
    # df_2019_Q4.show(5, truncate=False)

    # print("Second ZIP DataFrame:")
    # df_2020_Q1.show(5, truncate=False)
    # return all_dfs

    # Question 1 call
    result_df = calculate_avg_trip_duration_per_day(df_2020_Q1)

    # Question2 call
    result_df = calculate_trips_per_day(df_2020_Q1)

    # Question3 call
    result_df = find_most_popular_start_station(df_2020_Q1)

    # Question4 call
    result_df = find_top_3_stations_last_two_weeks(df_2020_Q1)

    # Question5 call
    result_df = avg_trip_duration_by_gender(df_2020_Q1)

    # Question6 call
    result_df = top_10_ages_longest_shortest_trips(df_2020_Q1)


if __name__ == "__main__":
    main()


# Question 1
def calculate_avg_trip_duration_per_day(df, output_path="reports/avg_trip_per_day.csv"):
    df = df.withColumn("date", F.to_date(F.col("start_time")))
    avg_trip_duration_per_day = (
        df.groupBy("date")
        .agg(F.avg("tripduration").alias("avg_trip_duration"))
        .orderBy("date")
    )
    avg_trip_duration_per_day.toPandas().to_csv(output_path, index=False)
    avg_trip_duration_per_day.show(10, truncate=False)
    return avg_trip_duration_per_day


# Question 2
def calculate_trips_per_day(df, output_path="reports/trips_per_day.csv"):
    df = df.withColumn("date", F.to_date(F.col("start_time")))
    trips_per_day = (
        df.groupBy("date")
        .agg(F.count("*").alias("total_trips"))
        .orderBy("date")
    )
    trips_per_day.toPandas().to_csv(output_path, index=False)
    trips_per_day.show(10, truncate=False)
    return trips_per_day


# Question 3

def find_most_popular_start_station(df, output_path="reports/most_popular_start_station.csv"):
    df = df.withColumn("month", F.month(F.col("start_time")))
    station_count = df.groupBy("month", "from_station_name").agg(F.count("*").alias("trip_count"))
    window_spec = Window.partitionBy("month").orderBy(F.col("trip_count").desc())
    ranked_stations = (
        station_count
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .orderBy("month")
    )
    ranked_stations.toPandas().to_csv(output_path, index=False)
    ranked_stations.show(10, truncate=False)
    return ranked_stations


# Question 4
def find_top_3_stations_last_two_weeks(df, output_path="reports/top_3_trip_stations_last_two_weeks.csv"):
    df = df.withColumn("start_date", F.to_date(F.col("start_time")))
    max_date = df.select(F.max("start_date")).collect()[0][0]
    two_weeks_ago = F.date_sub(F.lit(max_date), 14)
    df_last_two_weeks = df.filter(F.col("start_date") >= two_weeks_ago).alias("df_last_two_weeks")
    station_counts = (
        df_last_two_weeks.groupBy("start_date", "from_station_name")
        .agg(F.count("*").alias("trip_count"))
    )
    window_spec = Window.partitionBy("start_date").orderBy(F.desc("trip_count"))
    top_3_stations_per_day = (
        station_counts
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= 3)
        .orderBy("start_date", "rank")
    )
    top_3_stations_per_day.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    top_3_stations_per_day.show(truncate=False)
    return top_3_stations_per_day


# Question 5
def avg_trip_duration_by_gender(df, output_path="reports/male_or_female.csv"):
    total_avg = (
        df.filter(F.isnotnull(F.col("gender")))
        .groupBy("gender")
        .agg(F.avg("tripduration").alias("avg_trip_duration"))
    )
    total_avg.toPandas().to_csv(output_path, header=True, index=False)
    total_avg.show()
    return total_avg

# Question 6
def top_10_ages_longest_shortest_trips(df, output_path="reports/top_10_ages.csv"):
    age = df.withColumn("age", F.lit(2020) - F.col("birthyear"))
    age = age.filter((F.col("age") > 0) & (F.col("age") < 100))

    longest_trip = (
        age.orderBy(F.desc("tripduration"))
        .select("age", "tripduration")
        .limit(10)
        .withColumn("trip_type", F.lit("Longest"))
    )

    shortest_trip = (
        age.orderBy(F.asc("tripduration"))
        .select("age", "tripduration")
        .limit(10)
        .withColumn("trip_type", F.lit("Shortest"))
    )

    combined = longest_trip.union(shortest_trip)
    combined.toPandas().to_csv(output_path, index=False)
    combined.show(truncate=False)
    return combined


