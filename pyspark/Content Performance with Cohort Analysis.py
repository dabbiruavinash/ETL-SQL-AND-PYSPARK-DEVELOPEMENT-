def content_preformance_cohort_analysis():

       content_cohorts = content_df.alias("c")
              .filter(col("add_date") >= "2023-01-01")
              .withColumn("content_cohort", date_format("added_date", "yyyy-mm"))
              .join(viewing_history_df.alias("vh"), "content_id", "left")
              .withColumn("days_since_added", datediff("vh.start_time", "c.added_date"))
              .withColumn("intital_period", when(col("days_since_addded").between(0,30),1).otherwise(0))
              .withColumn("long_term_period", when(col("days_since_added").between(31,90),1).otherwise(0))

       intital_performance = content_cohorts
               .filter(col("intital_period") == 1)
               .groupBy("content_id", "title", "genre", "content_type", "content_cohort")
               .agg(
                      countDistinct("profile_id").alias("intital_viewers"),
                      avg("completion_percentage").alias("intital_completion_rate"))

       long_term_performace = content_cohorts
               .filter(col("long_term_period") == 1)
               .groupBy("content_id")
               .agg(
                      countDistinct("profile_id").alias("long_term_viewers"),
                      countDistinct(when(col("intital_period") == 1, "profile_id").otherwise(None)).alias("retained_viewers"))

       content_metrics = intital_performace.alias("ip")
               .join(long_term_performace.alias("ltp"), "content_id", "left")
               .withColumn("viewer_retention_rate",
                                     when(col("initial_viewers") > 0,
                                               (col("retained_viewers") / col("initial_viewers")) * 100).otherwise(0))

        result = content_metrics
               .groupBy("content_cohort", "genre", content_type")
               .agg(
                       count("*").alias("content_count"),
                       avg("initial_viewers").alias("avg_initial_viewers"),
                       avg("long_term_viewers").alias("avg_long_term_viewers"),
                       avg("viewer_retention_rate").alias("avg_retention_rate"),
                       avg("initial_completion_rate").alias("avg_initial_completion"),
                       sum(when(col("viewer_retention_rate") > 50, 1).otherwise(0)).alias("high_retention_content"),
                       sum(when(col("viewer_retention_rate").between(20,50),1).otherwise(0)).alias("medium_retention_content"),
                       sum(when(col("viewer_retention_rate") < 20, 1).otherwise(0)).alias("low_retention_content"))
                       .filter(col("content_count") >= 5)
                       .orderBy("content_cohort", desc("avg_retention_rate"))

        return result