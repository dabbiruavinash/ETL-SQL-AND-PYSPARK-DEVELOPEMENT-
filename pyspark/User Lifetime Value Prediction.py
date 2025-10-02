def user_lifetime_value_prediction():
    """Predict user lifetime value based on behavior patterns"""
    
    # User historical data
    user_historical_data = users_df.alias("u") \
        .join(user_profiles_df.alias("up"), "user_id", "left") \
        .join(viewing_history_df.alias("vh"), "profile_id", "left") \
        .join(content_df.alias("c"), "content_id", "left") \
        .join(watchlist_df.alias("wl"), "profile_id", "left") \
        .join(user_ratings_df.alias("ur"), "profile_id", "left") \
        .filter(col("u.signup_date") <= date_sub(current_date(), 90)) \
        .groupBy("u.user_id", "u.subscription_type", "u.signup_date", "u.country") \
        .agg(
            countDistinct(month("vh.start_time")).alias("active_months"),
            countDistinct("vh.content_id").alias("total_content_watched"),
            avg("vh.completion_percentage").alias("avg_completion_rate"),
            countDistinct("vh.device_type").alias("devices_used"),
            countDistinct("c.genre").alias("genres_watched"),
            sum(when(col("vh.start_time") >= date_sub(current_date(), 30), 1).otherwise(0)).alias("recent_30d_views"),
            countDistinct("wl.content_id").alias("watchlist_items"),
            countDistinct("ur.rating_id").alias("ratings_given")
        )
    
    # Subscription values
    subscription_value_df = spark.createDataFrame([
        ("basic", 9.99), ("premium", 15.99), ("family", 19.99)
    ], ["subscription_type", "monthly_value"])
    
    # LTV calculation
    user_ltv_calculation = user_historical_data.alias("uh") \
        .join(subscription_value_df.alias("sv"), "subscription_type") \
        .withColumn("churn_probability",
                   when(col("recent_30d_views") == 0, 0.6).otherwise(0) +
                   when(col("avg_completion_rate") < 40, 0.3).otherwise(0) +
                   when(col("genres_watched") < 3, 0.1).otherwise(0)) \
        .withColumn("engagement_score",
                   (col("total_content_watched") * 0.2 +
                    col("avg_completion_rate") * 0.3 +
                    col("devices_used") * 0.1 +
                    col("genres_watched") * 0.1 +
                    col("watchlist_items") * 0.1 +
                    col("ratings_given") * 0.1 +
                    (col("recent_30d_views") / greatest(col("active_months"), lit(1))) * 0.1))
    
    # Predicted LTV
    predicted_ltv = user_ltv_calculation \
        .withColumn("predicted_remaining_months",
                   when(col("engagement_score") > 0.8, 24)
                   .when(col("engagement_score") > 0.5, 12)
                   .when(col("engagement_score") > 0.2, 6)
                   .otherwise(3) * (1 - col("churn_probability"))) \
        .withColumn("historical_revenue", col("active_months") * col("monthly_value")) \
        .withColumn("predicted_future_ltv", col("predicted_remaining_months") * col("monthly_value")) \
        .withColumn("total_predicted_ltv", col("historical_revenue") + col("predicted_future_ltv")) \
        .withColumn("ltv_segment",
                   when(col("total_predicted_ltv") > 500, "VIP")
                   .when(col("total_predicted_ltv") > 200, "High Value")
                   .when(col("total_predicted_ltv") > 100, "Medium Value")
                   .otherwise("Low Value")) \
        .withColumn("retention_priority",
                   when((col("churn_probability") > 0.7) & (col("total_predicted_ltv") > 200), "High Priority Retention")
                   .when((col("churn_probability") > 0.5) & (col("total_predicted_ltv") > 100), "Medium Priority Retention")
                   .when(col("churn_probability") > 0.3, "Monitor")
                   .otherwise("Healthy"))
    
    result = predicted_ltv \
        .select("user_id", "subscription_type", "country", "engagement_score", 
               "churn_probability", "historical_revenue", "predicted_future_ltv",
               "total_predicted_ltv", "ltv_segment", "retention_priority") \
        .orderBy(desc("total_predicted_ltv"))
    
    return result