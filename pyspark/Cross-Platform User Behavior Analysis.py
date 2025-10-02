def cross_platform_behavior_analysis():
    """Analyze user behavior across different devices"""
    
    # User device patterns
    user_device_patterns = user_profiles_df.alias("up") \
        .join(users_df.alias("u"), "user_id") \
        .join(viewing_history_df.alias("vh"), "profile_id") \
        .filter(col("vh.start_time") >= date_sub(current_date(), 60)) \
        .groupBy("up.profile_id", "u.user_id", "u.subscription_type") \
        .agg(
            countDistinct("vh.device_type").alias("unique_devices_used"),
            count("vh.view_id").alias("total_views"),
            avg("vh.completion_percentage").alias("avg_completion_rate"),
            countDistinct("vh.content_id").alias("unique_content_watched"),
            avg(when(hour("vh.start_time").between(6, 18), 1).otherwise(0)).alias("day_viewing_ratio"),
            avg(when(hour("vh.start_time").between(19, 23) | hour("vh.start_time").between(0, 5), 1).otherwise(0)).alias("night_viewing_ratio")
        ) \
        .filter(col("total_views") >= 10)
    
    # Find primary device for each user
    primary_device_window = Window.partitionBy("profile_id").orderBy(desc("device_views"))
    primary_device = viewing_history_df.alias("vh") \
        .groupBy("profile_id", "device_type") \
        .agg(count("*").alias("device_views")) \
        .withColumn("rn", row_number().over(primary_device_window)) \
        .filter(col("rn") == 1) \
        .select("profile_id", col("device_type").alias("primary_device"))
    
    # Device switching frequency
    device_switching = viewing_history_df.alias("vh") \
        .select("profile_id", date_format("start_time", "yyyy-MM-dd").alias("view_date"), "device_type") \
        .distinct() \
        .groupBy("profile_id") \
        .agg(count("*").alias("device_switching_count"))
    
    # Combine all metrics
    device_engagement = user_device_patterns.alias("udp") \
        .join(primary_device.alias("pd"), "profile_id") \
        .join(device_switching.alias("ds"), "profile_id") \
        .withColumn("device_usage_category",
                   when(col("unique_devices_used") == 1, "Single Device")
                   .when(col("unique_devices_used") == 2, "Two Devices")
                   .otherwise("Multi-Device")) \
        .withColumn("switching_frequency",
                   when(col("device_switching_count") / greatest(lit(60), lit(1)) > 0.5, "High Switcher")
                   .otherwise("Low Switcher")) \
        .withColumn("engagement_score",
                   (col("unique_content_watched") * 0.3 +
                    col("avg_completion_rate") * 0.4 +
                    (col("day_viewing_ratio") + col("night_viewing_ratio")) * 0.3))
    
    # Final aggregation
    result = device_engagement \
        .groupBy("primary_device", "device_usage_category", "switching_frequency", "subscription_type") \
        .agg(
            count("*").alias("user_count"),
            avg("unique_content_watched").alias("avg_unique_content"),
            avg("avg_completion_rate").alias("avg_completion"),
            avg("day_viewing_ratio").alias("avg_day_viewing"),
            avg("night_viewing_ratio").alias("avg_night_viewing"),
            avg("engagement_score").alias("engagement_score")
        ) \
        .filter(col("user_count") >= 5) \
        .orderBy(desc("engagement_score"))
    
    return result