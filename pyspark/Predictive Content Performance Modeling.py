def predictive_content_performance():
    """Build predictive model to forecast content performance"""
    
    # Content features extraction
    content_features = content_df.alias("c") \
        .filter(col("release_year") >= year(current_date()) - 10) \
        .withColumn("seasonal_multiplier",
                   when((col("genre").isin(["Horror", "Thriller"])) & 
                        month(col("added_date")).isin([10, 11]), 1.2)
                   .when((col("genre").isin(["Romance", "Comedy"])) & 
                         month(col("added_date")).isin([2, 12]), 1.15)
                   .when((col("genre").isin(["Action", "Adventure"])) & 
                         month(col("added_date")).isin([6, 7]), 1.1)
                   .otherwise(1.0))
    
    # Actor popularity features (simplified - in reality would join with actors table)
    actor_popularity = content_df.alias("c1") \
        .groupBy("genre") \
        .agg(avg("imdb_rating").alias("genre_avg_rating"))
    
    # Historical performance metrics
    historical_performance = content_features.alias("cf") \
        .join(viewing_history_df.alias("vh"), "content_id", "left") \
        .filter(col("cf.added_date") <= date_sub(current_date(), 90)) \
        .groupBy("cf.content_id", "cf.title", "cf.content_type", "cf.genre", 
                "cf.release_year", "cf.duration_minutes", "cf.maturity_rating",
                "cf.imdb_rating", "cf.seasonal_multiplier") \
        .agg(
            countDistinct("vh.profile_id").alias("actual_viewers"),
            avg("vh.completion_percentage").alias("actual_completion_rate"),
            count("vh.view_id").alias("total_views"),
            countDistinct(date_format("vh.start_time", "yyyy-MM-dd")).alias("active_days"),
            countDistinct(when(col("vh.start_time").between(
                date_add(col("cf.added_date"), 31), 
                date_add(col("cf.added_date"), 90)), "vh.profile_id")).alias("long_term_viewers")
        )
    
    # Join with actor popularity
    historical_with_features = historical_performance.alias("hp") \
        .join(actor_popularity.alias("ap"), "genre") \
        .withColumn("predicted_success_score",
                   (col("imdb_rating") * 0.25 +
                    col("genre_avg_rating") * 0.20 +
                    col("actual_completion_rate") / 100 * 0.15 +
                    col("seasonal_multiplier") * 0.30)) \
        .withColumn("predicted_viewers",
                   (col("imdb_rating") * 1000 + 
                    col("genre_avg_rating") * 500 +
                    col("actual_completion_rate") * 10)) \
        .withColumn("performance_category",
                   when(col("actual_completion_rate") > 80, "High Performance")
                   .when(col("actual_completion_rate") > 60, "Medium Performance")
                   .otherwise("Low Performance"))
    
    # Model accuracy by genre
    model_accuracy = historical_with_features \
        .filter(col("actual_viewers") > 0) \
        .groupBy("genre") \
        .agg(
            count("*").alias("sample_size"),
            avg(abs((col("predicted_viewers") - col("actual_viewers")) / col("actual_viewers")) * 100).alias("genre_mape"),
            avg(abs(col("predicted_success_score") * 100 - col("actual_completion_rate"))).alias("completion_mae")
        ) \
        .filter(col("sample_size") >= 10)
    
    # Final predictions with confidence
    result = historical_with_features.alias("hwf") \
        .join(model_accuracy.alias("ma"), "genre") \
        .withColumn("prediction_confidence",
                   when(col("genre_mape") < 20, "High Confidence")
                   .when(col("genre_mape").between(20, 40), "Medium Confidence")
                   .otherwise("Low Confidence")) \
        .withColumn("acquisition_recommendation",
                   when((col("predicted_success_score") > 0.7) & (col("predicted_viewers") > 5000), "Strong Acquire")
                   .when((col("predicted_success_score") > 0.5) & (col("predicted_viewers") > 2000), "Consider Acquire")
                   .when((col("predicted_success_score") < 0.3) | (col("predicted_viewers") < 500), "Reject")
                   .otherwise("Further Analysis Needed")) \
        .select("content_id", "title", "content_type", "genre", "imdb_rating",
               "predicted_success_score", "predicted_viewers", "actual_viewers",
               "actual_completion_rate", "performance_category", "prediction_confidence",
               "acquisition_recommendation") \
        .filter(col("actual_viewers").isNotNull()) \
        .orderBy(desc("predicted_success_score"))
    
    return result