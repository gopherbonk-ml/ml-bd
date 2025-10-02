from dataclasses import dataclass
import warnings
from pyspark.sql import SparkSession, functions as F


@dataclass
class Args:
    input: str = "/Users/gopherbonk/ml-bd/lab1/data/orders_raw"
    out_avro: str = "/Users/gopherbonk/ml-bd/lab1/data/orders_avro"
    out_parquet: str = "/Users/gopherbonk/ml-bd/lab1/data/orders_parquet"
    deadletter: str = "/Users/gopherbonk/ml-bd/lab1/data/orders_deadletter"


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("clean_to_avro_parquet")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    warnings.filterwarnings("ignore")
    args = Args()
    spark = build_spark()

    df = (
        spark.read.option("header", True).csv(args.input)
        .withColumn("price", F.col("price").cast("int"))
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("lng", F.col("lng").cast("double"))
        .withColumn("Order_id", F.col("Order_id").cast("long"))
        .withColumn("datetime", F.to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("date", F.to_date("datetime"))
    )

    bad_cond = (
        (F.col("lat") == -999.999)
        | (F.col("lng") == -999.999)
        | F.col("facility_name").isNull()
        | F.col("menu_item").isNull()
        | F.col("Order_id").isNull()
    )

    bad = df.where(bad_cond)
    good = df.where(~bad_cond)

    (
        bad.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(args.deadletter)
    )

    (
        good.write.mode("overwrite")
        .partitionBy("date", "city")
        .format("avro")
        .save(args.out_avro)
    )

    (
        good.write.mode("overwrite")
        .partitionBy("date", "city")
        .parquet(args.out_parquet)
    )

    spark.stop()


if __name__ == "__main__":
    main()
