from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("userEngagementFunnelAnalysis").getOrCreate()

three_months_ago = date_sub(current_date(), 90)

user_engagement = users_df
.filter(col("signup_date") >= three_months_ago)
.alias("u")
.join(user_profiles_df.alias("up"), "user_id", "left")
.join(viewing_history_df.alias("vh"), col("up.profile_id") == col("vh.profile_id"), "left")
.join(watchlist_df.alias("wl"), col("up.profile_id") == col("wl.profile_id"), "left")
.groupBy("u.user_id", "u.subscription_type", "u.country")
.agg(
countDistinct("vh.content_id").alias("unique_content_watched"),
count("vh.view_id").alias("total_views"),
avg("vh.completion_percentage").alias("avg_completion_rate"),
max("vh.start_time").alias("last_watch_date"),
countDistinct("up.profile_id").alias("profiles_created"),
countDistinct("wl.content_id").alias("watchlist_items"))

engagement_segments = user_engagement
.withColumn("engagement_segment",
when(col("total_views") == 0, "Inactive")
.when(col("total_views") <= 5, "Light User")
.when(col("total_views") <= 20, "Medium User").otherwise("Heavy User"))
.withColumn("completion_segment",
when(col("avg_completion_rate") < 50, "Low completion")
.when(col("avg_completion_rate") < 80, "Medium Completion").otherwise("High completion"))

result = engagement_segments
.groupBy("subscription_type", "country", "engagement_segment", "completion_segment")
.agg(
count("*").alias("user_count"),
avg("unique_content_watched").alias("avg_unique_content"),
avg("total_views").alias("avg_total_views"),
avg("profiles_created").alias("avg_profiles").filter(col("user_count") > 10)

result_with_rollup = engagement_segments
.rollup("subscroption_type", "country", "engagement_segment", "completion_segment")
.agg(
count("*").alias("user_count"),
avg("unique_content_watched").alias("avg_unique_content"),
avg("total_views).alias("avg_total_views"),
avg("profiles_created").alias("avg_profiles"))
.filter(col("user_count") > 10)
.orderBy("subscription_type", desc("user_count"))

result_with_rollup.show()