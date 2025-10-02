from dataclasses import dataclass
from pathlib import Path
import warnings
from pyspark.sql import SparkSession, functions as F

@dataclass
class Args:
    avro_path: str = "/Users/gopherbonk/ml-bd/lab1/data/orders_avro"
    pg_url: str = "jdbc:postgresql://localhost:5432/labdb?sslmode=disable"
    pg_user: str = "lab"
    pg_pass: str = "lab"
    pg_schema: str = "public"
    pg_driver: str = "org.postgresql.Driver"
    spark_packages: str = "org.apache.spark:spark-avro_2.12:3.5.5,org.postgresql:postgresql:42.7.5"

def build_spark(a: Args) -> SparkSession:
    spark = (SparkSession.builder
             .appName("avro_to_postgres_3NF")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.jars.packages", a.spark_packages)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def write_jdbc(df, table, a: Args, mode="overwrite"):
    (df.write.format("jdbc")
       .option("url", a.pg_url)                    # уже с dbname и sslmode=disable
       .option("dbtable", f"{a.pg_schema}.{table}")
       .option("user", a.pg_user)
       .option("password", a.pg_pass)
       .option("driver", a.pg_driver)
       .mode(mode)
       .save())

def main():
    warnings.filterwarnings("ignore")
    a = Args()
    spark = build_spark(a)

    base = Path(a.avro_path).expanduser().resolve()
    df = spark.read.format("avro").load(str(base))

    order_id_col = F.coalesce(F.col("order_id"), F.col("Order_id")).alias("order_id")

    cities = df.select("city", "country").distinct()
    facilities = df.select("facility_name", "city", "lat", "lng").distinct()
    menu_items = df.select("menu_item", "price").distinct()
    orders = df.select(order_id_col, "datetime", "facility_name", "city").distinct()
    order_items = (df.withColumn("order_item_id", F.monotonically_increasing_id())
                     .select("order_item_id", order_id_col, "menu_item")
                     .distinct())

    write_jdbc(cities, "cities", a)
    write_jdbc(facilities, "facilities", a)
    write_jdbc(menu_items, "menu_items", a)
    write_jdbc(orders, "orders", a)
    write_jdbc(order_items, "order_items", a)

    spark.stop()

if __name__ == "__main__":
    main()
