def recommendation_system_performance():
    """Evaluate recommendation algorithm effectiveness"""
    
    # Track if recommendations were acted upon
    recommendation_performance = recommendations_df.alias("r") \
        .join(viewing_history_df.alias("vh"), 
              (col("r.profile_id") == col("vh.profile_id")) & 
              (col("r.content_id") == col("vh.content_id")) &
              (col("vh.start_time").between(col("r.created_date"), date_add(col("r.created_date"), 30))), 
              "left") \
        .withColumn("was_watched", when(col("vh.view_id").isNotNull(), 1).otherwise(0)) \
        .withColumn("hours_to_watch", 
                   when(col("was_watched") == 1,
                        hours_between(col("r.created_date"), min("vh.start_time").over(
                            Window.partitionBy("r.profile_id", "r.content_id")
                        )))
                   .otherwise(None)) \
        .join(user_profiles_df.alias("up"), "profile_id") \
        .join(users_df.alias("u"), "user_id")
    
    # User activity level calculation
    user_activity = viewing_history_df.alias("vh") \
        .join(user_profiles_df.alias("up"), "profile_id") \
        .groupBy("up.user_id") \
        .agg(
            count("*").alias("total_views"),
            countDistinct("content_id").alias("unique_content")
        ) \
        .withColumn("activity_level",
                   when(col("total_views") < 10, "Low")
                   .when(col("total_views") < 50, "Medium")
                   .otherwise("High"))
    
    # Algorithm metrics
    algorithm_metrics = recommendation_performance.alias("rp") \
        .join(user_activity.alias("ua"), "user_id") \
        .groupBy("algorithm_version") \
        .agg(
            count("*").alias("total_recommendations"),
            sum("was_watched").alias("accepted_recommendations"),
            (avg("was_watched") * 100).alias("acceptance_rate"),
            avg(when(col("was_watched") == 1, col("hours_to_watch"))).alias("avg_time_to_watch"),
            corr("recommendation_score", "was_watched").alias("score_correlation"),
            avg("ua.total_views").alias("avg_user_activity")
        )
    
    # User segment analysis
    user_segment_analysis = recommendation_performance.alias("rp") \
        .join(user_activity.alias("ua"), "user_id") \
        .groupBy("algorithm_version", "activity_level") \
        .agg(
            count("*").alias("recommendations_count"),
            (avg("was_watched") * 100).alias("segment_acceptance_rate"),
            avg("recommendation_score").alias("avg_recommendation_score")
        )
    
    # Combine results
    result = algorithm_metrics.alias("am") \
        .join(user_segment_analysis.alias("usa"), "algorithm_version") \
        .groupBy("am.algorithm_version", "total_recommendations", "acceptance_rate", 
                "avg_time_to_watch", "score_correlation") \
        .pivot("activity_level", ["Low", "Medium", "High"]) \
        .agg(first("segment_acceptance_rate")) \
        .orderBy(desc("acceptance_rate"))
    
    return result