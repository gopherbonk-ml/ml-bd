from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder
         .appName("fs_to_kafka")
         .getOrCreate())

schema = T.StructType([
    T.StructField("id", T.StringType(), False),
    T.StructField("name", T.StringType(), False),
    T.StructField("email", T.StringType(), False),
    T.StructField("country", T.StringType(), False),
    T.StructField("created_at", T.StringType(), False)
])

df = (spark.readStream
      .format("json")
      .schema(schema)
      .option("multiline", "true")
      .option("maxFilesPerTrigger", 1)
      .load("/shared/out"))

events = (df
          .withColumn("key", F.col("country").cast("string"))
          .withColumn("value", F.to_json(F.struct("*")))
          .selectExpr("CAST(key AS BINARY) as key", "CAST(value AS BINARY) as value"))

query = (events.writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "lab-rp1:9092,lab-rp2:9092,lab-rp3:9092")
         .option("topic", "lab.events")
         .option("checkpointLocation", "/shared/checkpoints/fs_to_kafka")
         .outputMode("append")
         .start())

query.awaitTermination()
