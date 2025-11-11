from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

import zipfile


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    zip_path = "data/hard-drive-2022-01-01-failures.csv.zip"

    with zipfile.ZipFile(zip_path, "r") as z:
        csv_filename = z.namelist()[0] 
        with z.open(csv_filename) as f:
            data = f.read().decode("utf-8")

    temp_path = "/tmp/temp_hard_drive.csv"
    with open(temp_path, "w", encoding="utf-8") as temp_file:
        temp_file.write(data)

    df = spark.read.csv(temp_path, header=True, inferSchema=True)

    df.show(5)
    df.printSchema()


    # Question 1
    def add_source_file_column(df, filename):
        return df.withColumn("source_file", F.lit(filename))
    df = add_source_file_column(df, "hard-drive-2022-01-01-failures.csv")
    # df.show(5, truncate=False)



    # Question 2
    def add_file_date_column(df):
        return df.withColumn(
            "file_date",
            F.to_date(
                F.regexp_extract(F.col("source_file"), r"(\d{4}-\d{2}-\d{2})", 1),
                "yyyy-MM-dd"
            )
        )
    df = add_file_date_column(df)
    # df.select("source_file", "file_date").show(5, truncate=False)


    # Question 3
    def add_brand_column(df):
        return df.withColumn(
            "brand",
            F.when(
                F.size(F.split(F.col("model"), " ")) > 1,
                F.split(F.col("model"), " ")[0]
            ).otherwise("unknown")
        )
    df = add_brand_column(df)
    # df.show(5)

    # Question 4
    def add_storage_ranking(df):
        df = df.withColumn("capacity_bytes", F.col("capacity_bytes").cast("long"))
        ranked_df = df.filter(F.col("brand").isNotNull() & F.col("capacity_bytes").isNotNull())
        windowSpec = Window.partitionBy("brand").orderBy(F.desc("capacity_bytes"))
        return ranked_df.withColumn("storage_ranking", F.dense_rank().over(windowSpec))
    ranked_df = add_storage_ranking(df)
    # ranked_df.show(10)

    # Question 5
    def add_primary_key(df):
        return df.withColumn("primary_key", F.hash(F.col("date"), F.col("serial_number")))
    df = add_primary_key(df)
    # df.show(5)



if __name__ == "__main__":
    main()


