def churn_prediction_analysis():
    """Identify users at high risk of churning"""
    
    # User engagement metrics
    user_engagement_metrics = users_df.alias("u") \
        .join(user_profiles_df.alias("up"), "user_id") \
        .join(viewing_history_df.alias("vh"), "profile_id") \
        .filter(col("u.signup_date") <= date_sub(current_date(), 60)) \
        .withColumn("recent_period", when(col("vh.start_time") >= date_sub(current_date(), 30), 1).otherwise(0)) \
        .withColumn("historical_period", when(col("vh.start_time").between(
            date_sub(current_date(), 90), date_sub(current_date(), 31)), 1).otherwise(0)) \
        .groupBy("u.user_id", "u.subscription_type", "u.country", "u.signup_date") \
        .agg(
            sum("recent_period").alias("recent_views"),
            sum("historical_period").alias("historical_views"),
            countDistinct(when(col("recent_period") == 1, date_format("vh.start_time", "yyyy-MM-dd"))).alias("active_days_recent"),
            countDistinct("vh.content_id").alias("unique_content_60d"),
            avg("vh.completion_percentage").alias("avg_completion_rate"),
            countDistinct("vh.device_type").alias("unique_devices")
        )
    
    # Churn risk calculation
    churn_risk_calculation = user_engagement_metrics \
        .withColumn("activity_decline_ratio",
                   when(col("historical_views") > 0,
                        (col("historical_views") - col("recent_views")) / col("historical_views"))
                   .otherwise(0)) \
        .withColumn("churn_risk_score",
                   when(col("recent_views") == 0, 0.4).otherwise(0) +
                   when(col("activity_decline_ratio") > 0.7, 0.3).otherwise(0) +
                   when(col("avg_completion_rate") < 50, 0.2).otherwise(0) +
                   when(col("unique_devices") == 1, 0.1).otherwise(0)) \
        .withColumn("engagement_segment",
                   when(col("recent_views") == 0, "Inactive")
                   .when(col("activity_decline_ratio") > 0.5, "Declining")
                   .when(col("unique_content_60d") < 5, "Limited Content")
                   .otherwise("Active"))
    
    # Final aggregation
    result = churn_risk_calculation \
        .groupBy("subscription_type", "country", "engagement_segment") \
        .agg(
            count("*").alias("user_count"),
            avg("churn_risk_score").alias("avg_risk_score"),
            sum(when(col("churn_risk_score") > 0.7, 1).otherwise(0)).alias("high_risk_users"),
            sum(when(col("churn_risk_score").between(0.4, 0.7), 1).otherwise(0)).alias("medium_risk_users"),
            sum(when(col("churn_risk_score") < 0.4, 1).otherwise(0)).alias("low_risk_users"),
            avg("recent_views").alias("avg_recent_views"),
            avg("historical_views").alias("avg_historical_views"),
            avg("avg_completion_rate").alias("avg_completion"),
            avg("unique_content_60d").alias("avg_unique_content")
        ) \
        .filter(col("user_count") >= 10) \
        .orderBy("subscription_type", "country", desc("avg_risk_score"))
    
    return result