import polars as p1

# Question2 part 2
def rides_per_day_func(lazy_df):
    rides_per_day = (
        lazy_df.with_columns([
            p1.col("started_at").dt.date().alias("date")
        ])
        .group_by("date")
        .agg(p1.count().alias("ride_count"))
        .sort("ride_count", descending=True)
        .collect()
    )
    return rides_per_day


# Question 2nd part 3rd
def weekly_stats_func(lazy_df):
    lazy_df = lazy_df.with_columns([
        p1.col("started_at").dt.year().alias("Year"),
        p1.col("started_at").dt.week().alias("Week_number")
    ])
    rides_per_week = (
        lazy_df.group_by(["Year", "Week_number"])
        .agg(p1.len().alias("Total_rides_per_week"))
    )
    weekly_stats = (
        rides_per_week
        .select([
            p1.col("Total_rides_per_week").mean().alias("Avg_rides_per_week"),
            p1.col("Total_rides_per_week").max().alias("Max_rides_per_week"),
            p1.col("Total_rides_per_week").min().alias("Min_rides_per_week")
        ])
        .collect()
    )
    return weekly_stats


# Question 2nd part 4th
def rides_diff_from_last_week(lazy_df):
    rides_per_day = (
        lazy_df
        .with_columns(p1.col("started_at").dt.date().alias("date"))
        .group_by("date")
        .agg(p1.len().alias("Total_rides"))
        .sort("date")
        .collect()
    )

    rides_per_day = rides_per_day.with_columns([
        p1.col("Total_rides").shift(7).alias("Rides_last_week")
    ])

    rides_per_day = rides_per_day.with_columns([
        (p1.col("Total_rides").cast(p1.Int64) - p1.col("Rides_last_week").cast(p1.Int64))
            .alias("Diff_from_last_week")
    ])
    return rides_per_day



def main():
    # Question 1
    lazy_df = p1.scan_csv("data/202306-divvy-tripdata.csv", dtypes={"start_station_id": p1.Utf8, "end_station_id": p1.Utf8}, try_parse_dates=True)

    # question2 part 1
    lazy_df = lazy_df.with_columns([
        p1.col("ride_id").cast(p1.Utf8),
        p1.col("rideable_type").cast(p1.Utf8),
        p1.col("member_casual").cast(p1.Utf8),
        
        p1.col("started_at"),
        p1.col("ended_at"),
        
        p1.col("start_station_name").cast(p1.Utf8),
        p1.col("start_station_id").cast(p1.Utf8),
        p1.col("end_station_name").cast(p1.Utf8),
        p1.col("end_station_id").cast(p1.Utf8),

        p1.col("start_lat").cast(p1.Float64),
        p1.col("start_lng").cast(p1.Float64),
        p1.col("end_lat").cast(p1.Float64),
        p1.col("end_lng").cast(p1.Float64)
    ])

    df = lazy_df.collect()
    # print(df.schema)

    # Question 2nd part2
    rides_per_day =  rides_per_day_func(lazy_df)
    print(rides_per_day)


    # Question 2nd part3
    weekly_stats = weekly_stats_func(lazy_df)
    print(weekly_stats)

    # Question 2nd part 4
    rides_diff = rides_diff_from_last_week(lazy_df)
    print(rides_diff)


if __name__ == "__main__":
    main()
