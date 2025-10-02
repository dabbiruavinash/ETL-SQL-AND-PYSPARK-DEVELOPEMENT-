def binge_watching_analysis():
    """Identify and analyze binge-watching patterns"""
    
    # Session grouping for binge detection
    viewing_sessions = viewing_history_df.alias("vh") \
        .join(content_df.alias("c"), "content_id") \
        .withColumn("previous_end_time", 
                   lag("end_time").over(Window.partitionBy("profile_id", "content_id").orderBy("start_time"))) \
        .withColumn("time_gap_minutes",
                   when(col("previous_end_time").isNotNull(),
                        (unix_timestamp("start_time") - unix_timestamp("previous_end_time")) / 60)
                   .otherwise(None))
    
    # Binge episode identification
    binge_episodes = viewing_sessions \
        .filter((col("time_gap_minutes").isNull()) | (col("time_gap_minutes") < 240)) \
        .groupBy("profile_id", "content_id", "content_type", "genre") \
        .agg(
            min("start_time").alias("binge_start"),
            max("end_time").alias("binge_end"),
            count("*").alias("episodes_in_binge"),
            sum("duration_minutes").alias("total_binge_minutes"),
            avg("time_gap_minutes").alias("avg_time_between_episodes"),
            max("time_gap_minutes").alias("max_time_gap")
        ) \
        .filter(col("episodes_in_binge") >= 3) \
        .withColumn("binge_intensity",
                   when(col("avg_time_between_episodes") < 60, "High Intensity Binge")
                   .when(col("avg_time_between_episodes") < 180, "Moderate Binge")
                   .otherwise("Casual Viewing"))
    
    # User binge behavior analysis
    user_binge_behavior = binge_episodes.alias("be") \
        .join(user_profiles_df.alias("up"), "profile_id") \
        .join(users_df.alias("u"), "user_id") \
        .groupBy("be.profile_id", "u.user_id", "u.subscription_type", "u.country") \
        .agg(
            countDistinct("be.content_id").alias("total_binge_sessions"),
            avg("be.episodes_in_binge").alias("avg_episodes_per_binge"),
            avg("be.total_binge_minutes").alias("avg_binge_duration_minutes"),
            avg(when(hour("binge_start").between(18, 23), 1).otherwise(0)).alias("evening_binge_ratio"),
            avg(when(dayofweek("binge_start").isin(1, 7), 1).otherwise(0)).alias("weekend_binge_ratio"),
            countDistinct(when(col("genre") == "Drama", "be.content_id")).alias("drama_binges"),
            countDistinct(when(col("genre") == "Comedy", "be.content_id")).alias("comedy_binges"),
            countDistinct(when(col("content_type") == "tv_show", "be.content_id")).alias("tv_show_binges")
        )
    
    # Content-level binge analysis
    binge_content_analysis = binge_episodes.alias("be") \
        .join(content_df.alias("c"), "content_id") \
        .groupBy("c.content_id", "c.title", "c.genre", "c.content_type") \
        .agg(
            countDistinct("be.profile_id").alias("total_binge_viewers"),
            avg("be.episodes_in_binge").alias("avg_binge_length"),
            avg("be.total_binge_minutes").alias("avg_binge_duration"),
            avg("be.avg_time_between_episodes").alias("avg_time_between_episodes")
        ) \
        .filter(col("total_binge_viewers") >= 10)
    
    # First episode completion rate (binge trigger)
    first_episode_completion = viewing_history_df.alias("vh") \
        .withColumn("episode_rank", 
                   row_number().over(Window.partitionBy("profile_id", "content_id").orderBy("start_time"))) \
        .filter(col("episode_rank") == 1) \
        .groupBy("content_id") \
        .agg(avg("completion_percentage").alias("first_episode_completion_rate"))
    
    # Combine all binge metrics
    result = binge_content_analysis.alias("bca") \
        .join(first_episode_completion.alias("fec"), "content_id", "left") \
        .withColumn("bingeability_score",
                   (col("total_binge_viewers") * 0.3 +
                    col("avg_binge_length") * 0.25 +
                    coalesce(col("first_episode_completion_rate"), lit(0)) * 0.25 +
                    col("avg_time_between_episodes") * 0.2)) \
        .withColumn("marketing_recommendation",
                   when(col("bingeability_score") > 8, "Promote as Binge-Worthy")
                   .when(col("first_episode_completion_rate") > 80, "Strong First Episode - Push Series")
                   .when(col("avg_binge_length") > 5, "High Completion - Feature in Collections")
                   .otherwise("Standard Promotion")) \
        .select("content_id", "title", "genre", "content_type", "total_binge_viewers",
               "avg_binge_length", "first_episode_completion_rate", "bingeability_score",
               "marketing_recommendation") \
        .orderBy(desc("bingeability_score"))
    
    return result