from dataclasses import dataclass
from pathlib import Path
import warnings
from pyspark.sql import SparkSession, functions as F


@dataclass
class Args:
    input_avro: str = "/Users/gopherbonk/ml-bd/lab1/data/orders_avro"
    pg_url: str = "jdbc:postgresql://localhost:5432/labdb?sslmode=disable"
    pg_user: str = "lab"
    pg_pass: str = "lab"
    pg_schema: str = "public"
    pg_driver: str = "org.postgresql.Driver"
    spark_packages: str = "org.apache.spark:spark-avro_2.12:3.5.5,org.postgresql:postgresql:42.7.5"


def build_spark(a: Args) -> SparkSession:
    spark = (
        SparkSession.builder.appName("load_star_to_pg")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.packages", a.spark_packages)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def write_jdbc(df, table, a: Args, mode="overwrite"):
    (
        df.write.format("jdbc")
        .option("url", a.pg_url)
        .option("dbtable", f"{a.pg_schema}.{table}")
        .option("user", a.pg_user)
        .option("password", a.pg_pass)
        .option("driver", a.pg_driver)
        .mode(mode)
        .save()
    )


def main():
    warnings.filterwarnings("ignore")
    a = Args()
    spark = build_spark(a)

    base = Path(a.input_avro).expanduser().resolve()
    df = spark.read.format("avro").load(str(base))

    dt = F.coalesce(F.to_timestamp("datetime"), F.to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))
    order_id = F.coalesce(F.col("Order_id"), F.col("order_id")).alias("order_id")

    ddate = (
        df.select(F.to_date(dt).alias("date"))
        .dropDuplicates()
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("date_key", (F.col("year") * 10000 + F.col("month") * 100 + F.col("day")).cast("int"))
        .select("date_key", "date", "year", "month", "day")
    )

    dcity = (
        df.select("city", "country")
        .dropDuplicates()
        .withColumn("city_key", F.monotonically_increasing_id())
        .select("city_key", "city", "country")
    )

    df_fac = (
        df.select("facility_name", "city", "lat", "lng")
        .dropDuplicates()
        .join(dcity.select("city_key", "city"), "city")
        .withColumn("facility_key", F.monotonically_increasing_id())
        .select("facility_key", "facility_name", "city_key", "lat", "lng")
    )

    dmenu = (
        df.select("menu_item")
        .dropDuplicates()
        .withColumn("menu_key", F.monotonically_increasing_id())
        .select("menu_key", "menu_item")
    )

    fact = (
        df.withColumn("dt_ts", dt)
        .withColumn(
            "date_key",
            (F.year("dt_ts") * 10000 + F.month("dt_ts") * 100 + F.dayofmonth("dt_ts")).cast("int"),
        )
        .join(dcity, ["city", "country"])
        .join(df_fac.select("facility_key", "facility_name", "city_key"), ["facility_name", "city_key"])
        .join(dmenu, "menu_item")
        .select(
            order_id.alias("order_id"),
            F.col("datetime"),
            "date_key",
            "city_key",
            "facility_key",
            "menu_key",
            "price",
        )
    )

    write_jdbc(ddate, "dim_date", a)
    write_jdbc(dcity, "dim_city", a)
    write_jdbc(df_fac, "dim_facility", a)
    write_jdbc(dmenu, "dim_menu", a)
    write_jdbc(fact, "fact_sales", a)

    spark.stop()


if __name__ == "__main__":
    main()
