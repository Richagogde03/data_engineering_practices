import duckdb
import os

# 3rd, part1
def get_electric_cars_per_city(con):
    query = """
    SELECT City, COUNT("Electric Vehicle Type") AS electric_vehicle_count
    FROM electric_vehicles
    GROUP BY City
    """
    result_df = con.execute(query).fetchdf()
    return result_df

# 3rd, part 2
def top_3_vehicles(con):
    return con.execute("""
        SELECT "Make", "Model", COUNT(*) AS total_count
        FROM electric_vehicles
        GROUP BY "Make", "Model"
        ORDER BY total_count DESC
        LIMIT 3
    """).fetchdf()

# 3rd, part 3
def most_popular_by_postalcode(con):
    return con.execute("""
        SELECT "Make", "Model", "Postal Code", Total_count 
        FROM (
            SELECT "Make",
                   "Model",
                   "Postal Code",
                   COUNT(*) as Total_count,
                   ROW_NUMBER() OVER (PARTITION BY "Postal Code" ORDER BY COUNT(*) DESC) as rn
            FROM electric_vehicles
            GROUP BY "Make", "Model", "Postal Code"
        ) sub
        WHERE rn = 1
        ORDER BY "Postal Code"
    """).fetch_df()


# 3rd, part4
def save_electric_cars_by_year_parquet(con):
    BASE_DIR = os.getcwd()
    output_folder = os.path.join(BASE_DIR, "reports", "electric_cars_by_year_parquet")

    query = """
    SELECT "Make","Model", count("Make") as electric_car_count, "Model Year"
    FROM electric_vehicles
    GROUP BY "Model Year", "Make", "Model"
    ORDER BY "Model Year"
    """
    
    result_df = con.execute(query).fetchdf()
    print(result_df)

    for year, df_year in result_df.groupby("Model Year"):
        year_folder = os.path.join(output_folder, f"Model_Year={year}")
        os.makedirs(year_folder, exist_ok=True)
        df_year.to_parquet(os.path.join(year_folder, "data.parquet"), engine="pyarrow", index=False)

    return result_df


def main():
    BASE_DIR = os.getcwd()

    csv_file = os.path.join(BASE_DIR, "data", "Electric_Vehicle_Population_Data.csv")
    print("CSV path:", csv_file)

    con = duckdb.connect()

    # 1st, creating table
    con.execute("""
    CREATE TABLE IF NOT EXISTS electric_vehicles (
        VIN VARCHAR,
        County VARCHAR,
        City VARCHAR,
        State CHAR(2),
        "Postal Code" INTEGER,
        "Model Year" INTEGER,
        Make VARCHAR,
        Model VARCHAR,
        "Electric Vehicle Type" VARCHAR,
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility" VARCHAR,
        "Electric Range" INTEGER,
        "Base MSRP" DOUBLE,
        "Legislative District" INTEGER,
        "DOL Vehicle ID" BIGINT,
        "Vehicle Location" VARCHAR,
        "Electric Utility" VARCHAR,
        "2020 Census Tract" BIGINT
    )
    """)

    # 2nd, loading data into the table from csv file
    con.execute(f"""
    COPY electric_vehicles
    FROM '{csv_file}'
    (HEADER, DELIMITER ',')
    """)

    result = con.execute("SELECT * FROM electric_vehicles LIMIT 5").fetchdf()
    # print(result)

    # 3rd, part1
    df_per_city = get_electric_cars_per_city(con)
    print(df_per_city)

    # 3rd, part2
    top_3 = top_3_vehicles(con)
    # print(top_3)

    # 3rd, part 3
    most_p_with_postalcode = most_popular_by_postalcode(con)
    print(most_p_with_postalcode)

    # 3rd, part 4
    df = save_electric_cars_by_year_parquet(con)
    # print(df)



if __name__ == "__main__":
    main()




