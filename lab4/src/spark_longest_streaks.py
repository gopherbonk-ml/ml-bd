import argparse, glob, os
from pyspark.sql import SparkSession, functions as F, Window as W


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="folder with *.csv per video")
    ap.add_argument("--out", required=True)
    ap.add_argument("--topn", type=int, default=10)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("skeet_longest_streaks").getOrCreate())
    df = (spark.read.option("header", True).csv(os.path.join(args.events, "*.csv")))

    df = (df
          .withColumn("t_sec", F.col("t_sec").cast("double"))
          .withColumn("is_hit", (F.col("state") == F.lit("broken")).cast("int")))

    w_vid = W.partitionBy("video_file").orderBy("t_sec")
    df2 = (df
           .withColumn("miss_flag", (1 - F.col("is_hit")).cast("int"))
           .withColumn("miss_cum", F.sum("miss_flag").over(w_vid))
           .withColumn("grp", F.concat_ws("_", F.col("video_file"), F.col("miss_cum")))
          )

    hits = df2.filter(F.col("is_hit") == 1)
    grp_sizes = (hits.groupBy("video_file", "grp")
                    .agg(F.count("*").alias("streak_len"),
                         F.first("t_sec").alias("start_t"),
                         F.max("t_sec").alias("end_t")))

    per_video = (grp_sizes
                 .withColumn("rn", F.row_number().over(W.partitionBy("video_file").orderBy(F.desc("streak_len"), F.asc("start_t"))))
                 .filter(F.col("rn") == 1)
                 .drop("rn"))

    topn = grp_sizes.orderBy(F.desc("streak_len"), F.asc("video_file")).limit(args.topn)

    per_video.write.mode("overwrite").option("header", True).csv(os.path.join(args.out, "per_video"))
    topn.write.mode("overwrite").option("header", True).csv(os.path.join(args.out, "topn"))

    print("Saved results to", args.out)
    spark.stop()

if __name__ == "__main__":
    main()