from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, explode, lower, regexp_extract, split, expr

spark = SparkSession.builder.appName("hashtagApp").getOrCreate()

df = spark.read.json("smallTwitter.json")

all_text_df = df.select(concat_ws(" ", *[expr(f"CAST({c} AS STRING)") for c in df.columns]).alias("all_text"))

words_df = all_text_df.select(explode(split(lower(col("all_text")), "\\s+")).alias("word"))
hashtags_df = words_df.filter(col("word").rlike("^#\\w+")) \
    .select(regexp_extract('word', r'#(\w+)', 1).alias('hashtag'))

hashtag_counts = hashtags_df.groupBy("hashtag").count()

sorted_hashtags = hashtag_counts.orderBy(col("count").desc()).limit(20)

sorted_hashtags.coalesce(1).write.option("header", "true").csv("hashtags_output")
print(sorted_hashtags.show())

spark.stop()