from dataclasses import dataclass
from pathlib import Path
import warnings
from pyspark.sql import SparkSession, functions as F


@dataclass
class Args:
    input_parquet: str = "/Users/gopherbonk/ml-bd/lab1/data/orders_parquet"
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "mlbd"
    mongo_collection: str = "orders"
    spark_packages: str = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"


def build_spark(a: Args) -> SparkSession:
    spark = (SparkSession.builder
        .appName("orders_to_mongo")
        .config("spark.ui.showConsoleProgress","false")
        .config("spark.jars", "/path/mongo/mongo-spark-connector_2.12-10.5.0.jar,/path/mongo/mongodb-driver-sync-4.11.1.jar,/path/mongo/mongodb-driver-core-4.11.1.jar,/path/mongo/bson-4.11.1.jar")
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.5.0")
        .config("spark.mongodb.write.connection.uri","mongodb://localhost:27017")
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    warnings.filterwarnings("ignore")
    a = Args()
    spark = build_spark(a)

    base = Path(a.input_parquet).expanduser().resolve()
    df = spark.read.parquet(str(base))

    order_id = F.coalesce(F.col("order_id"), F.col("Order_id")).alias("order_id")
    price_i = F.col("price").cast("int")
    items = F.collect_list(F.struct(F.col("menu_item"), price_i.alias("price")))

    agg = (
        df.withColumn("order_id", order_id)
        .groupBy("order_id", "city", "country", "facility_name", "lat", "lng")
        .agg(
            F.min("datetime").alias("datetime"),
            F.sum(price_i).alias("order_total"),
            items.alias("items"),
        )
        .select(
            F.col("order_id").alias("_id"),
            "city",
            "country",
            "facility_name",
            "lat",
            "lng",
            "datetime",
            "order_total",
            "items",
        )
    )

    (agg.write.format("mongodb")
   .option("spark.mongodb.write.database", "mlbd")
   .option("spark.mongodb.write.collection", "orders")
   .mode("overwrite")
   .save())

    spark.stop()


if __name__ == "__main__":
    main()
