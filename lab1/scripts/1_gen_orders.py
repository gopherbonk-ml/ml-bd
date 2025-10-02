from dataclasses import dataclass
from datetime import datetime, timedelta
import os
import random

from pyspark.sql import SparkSession, functions as F, types as T
import warnings


CITIES = [
    ("Moscow", "Russia", 55.7558, 37.6173),
    ("Saint Petersburg", "Russia", 59.9311, 30.3609),
    ("Novosibirsk", "Russia", 55.0084, 82.9357),
    ("Yekaterinburg", "Russia", 56.8389, 60.6057),
    ("Kazan", "Russia", 55.7963, 49.1088),
    ("Nizhny Novgorod", "Russia", 56.2965, 43.9361),
    ("Chelyabinsk", "Russia", 55.1644, 61.4368),
    ("Samara", "Russia", 53.1959, 50.1008),
    ("Rostov-on-Don", "Russia", 47.2357, 39.7015),
    ("Ufa", "Russia", 54.7388, 55.9721),
]

MENU = [
    ("Pizza", 450), ("Tea", 120), ("Apple Juice", 180), ("Burger", 350),
    ("Salad", 250), ("Soup", 220), ("Pasta", 390), ("Cake", 400),
    ("Coffee", 200), ("Ice Cream", 160),
]

FACILITIES_PER_CITY = 30


@dataclass
class Args:
    rows: int = 1_000_000
    out: str = "/Users/gopherbonk/ml-bd/lab1/data/orders_raw"


def generate_facilities() -> list[dict]:
    facilities = []
    for city, country, base_lat, base_lng in CITIES:
        for i in range(1, FACILITIES_PER_CITY + 1):
            facilities.append(
                {
                    "city": city,
                    "country": country,
                    "facility_name": f"Cafe {city} {i}",
                    "lat": base_lat + (random.random() * 0.02 - 0.01),
                    "lng": base_lng + (random.random() * 0.02 - 0.01),
                }
            )
    return facilities


def generate_order_data(rows: int, facilities: list[dict]) -> list[tuple]:
    data = []
    start_dt = datetime(2024, 1, 1)
    seconds_in_year = 365 * 24 * 3600
    generated = 0
    while generated < rows:
        order_id = random.randint(10_000, 1_010_000)
        items_cnt = random.randint(1, 5)
        for _ in range(items_cnt):
            if generated >= rows:
                break
            facility = random.choice(facilities)
            if random.random() < 0.1:
                menu_item, price = random.choice(MENU)
            else:
                menu_item, price = "Coffee", 200
            dt = start_dt + timedelta(seconds=random.randint(0, seconds_in_year))
            dirty = random.random()
            facility_name_val = None if dirty < 0.05 else facility["facility_name"]
            menu_item_val = None if 0.05 <= dirty < 0.08 else menu_item
            order_id_val = None if 0.08 <= dirty < 0.10 else order_id
            lat_val = -999.999 if random.random() < 0.05 else facility["lat"]
            lng_val = -999.999 if random.random() < 0.05 else facility["lng"]
            data.append(
                (
                    order_id_val,
                    facility["city"],
                    facility["country"],
                    facility_name_val,
                    lat_val,
                    lng_val,
                    menu_item_val,
                    price,
                    dt.strftime("%Y-%m-%d %H:%M:%S"),
                )
            )
            generated += 1
    return data


def build_spark() -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = "python3.10"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3.10"
    spark = (
        SparkSession.builder.appName("gen_orders_csv")
        .config("spark.pyspark.python", "python3.10")
        .config("spark.pyspark.driver.python", "python3.10")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    warnings.filterwarnings("ignore")
    args = Args()
    spark = build_spark()

    facilities = generate_facilities()
    order_data = generate_order_data(args.rows, facilities)

    schema = T.StructType(
        [
            T.StructField("Order_id", T.LongType(), True),
            T.StructField("city", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("facility_name", T.StringType(), True),
            T.StructField("lat", T.DoubleType(), True),
            T.StructField("lng", T.DoubleType(), True),
            T.StructField("menu_item", T.StringType(), True),
            T.StructField("price", T.IntegerType(), True),
            T.StructField("datetime", T.StringType(), True),
        ]
    )

    df = spark.createDataFrame(order_data, schema=schema)
    (
        df.coalesce(200)
        .write.option("header", True)
        .mode("overwrite")
        .csv(args.out)
    )

    print(f"Generated: {len(order_data)}")
    spark.stop()


if __name__ == "__main__":
    main()
