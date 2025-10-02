def subscription_change_analysis():
    """Analyze subscription plan changes and predict future behavior"""
    
    # Hypothetical subscription changes table
    subscription_changes_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("old_subscription", StringType(), True),
        StructField("new_subscription", StringType(), True),
        StructField("change_date", DateType(), True),
        StructField("change_type", StringType(), True),
        StructField("reason_code", StringType(), True)
    ])
    
    subscription_changes_df = spark.table("subscription_changes")
    
    # User metrics before change
    user_metrics_before_change = subscription_changes_df.alias("sc") \
        .join(viewing_history_df.alias("vh"), 
              (col("sc.user_id") == user_profiles_df.alias("up").select("user_id")) &
              (col("vh.profile_id") == col("up.profile_id")) &
              (col("vh.start_time").between(date_sub(col("sc.change_date"), 30), col("sc.change_date")))) \
        .groupBy("sc.user_id", "sc.old_subscription", "sc.new_subscription", 
                "sc.change_date", "sc.change_type", "sc.reason_code") \
        .agg(
            countDistinct("vh.content_id").alias("content_watched_30d"),
            avg("vh.completion_percentage").alias("completion_rate_30d")
        )
    
    # User demographics and usage patterns
    user_demographics = subscription_changes_df.alias("sc") \
        .join(users_df.alias("u"), "user_id") \
        .join(user_profiles_df.alias("up"), "user_id") \
        .join(viewing_history_df.alias("vh"), "profile_id") \
        .filter(col("vh.start_time").between(date_sub(col("sc.change_date"), 60), col("sc.change_date"))) \
        .groupBy("sc.user_id", "sc.change_date", "u.country", "u.age", "u.signup_date") \
        .agg(
            countDistinct("up.profile_id").alias("profiles_created"),
            countDistinct("vh.device_type").alias("devices_used")
        ) \
        .withColumn("days_since_signup", datediff("change_date", "signup_date"))
    
    # Change analysis with financial impact
    subscription_values = {
        "basic": 9.99, "premium": 15.99, "family": 19.99
    }
    
    change_analysis = user_metrics_before_change.alias("um") \
        .join(user_demographics.alias("ud"), "user_id") \
        .withColumn("revenue_impact",
                   coalesce(lit(subscription_values.get(col("new_subscription"), 0)), lit(0)) -
                   coalesce(lit(subscription_values.get(col("old_subscription"), 0)), lit(0))) \
        .withColumn("engagement_level",
                   when((col("content_watched_30d") > 20) & (col("completion_rate_30d") > 80), "High Engagement")
                   .when((col("content_watched_30d") < 5) & (col("completion_rate_30d") < 50), "Low Engagement")
                   .otherwise("Moderate Engagement")) \
        .withColumn("change_pattern",
                   when((col("change_type") == "upgrade") & (col("days_since_signup") < 30), "Early Upgrade")
                   .when((col("change_type") == "upgrade") & (col("content_watched_30d") > 15), "Usage-Based Upgrade")
                   .when((col("change_type") == "downgrade") & (col("content_watched_30d") < 5), "Underutilization Downgrade")
                   .otherwise("Other"))
    
    # Success rate calculation (users who didn't churn in 60 days)
    success_calculation = change_analysis.alias("ca") \
        .join(subscription_changes_df.alias("sc2"), 
              (col("ca.user_id") == col("sc2.user_id")) &
              (col("sc2.change_type") == "cancellation") &
              (col("sc2.change_date").between(col("ca.change_date"), date_add(col("ca.change_date"), 60))), "left") \
        .withColumn("successful_change", when(col("sc2.user_id").isNull(), 1).otherwise(0))
    
    # Upgrade predictors analysis
    upgrade_predictors = success_calculation \
        .groupBy("change_type", "change_pattern", "country") \
        .agg(
            count("*").alias("change_count"),
            avg("content_watched_30d").alias("avg_content_watched"),
            avg("completion_rate_30d").alias("avg_completion_rate"),
            avg("days_since_signup").alias("avg_days_since_signup"),
            avg("profiles_created").alias("avg_profiles"),
            avg("devices_used").alias("avg_devices"),
            (sum("successful_change") * 100.0 / count("*")).alias("success_rate_pct")
        ) \
        .filter(col("change_count") >= 10)
    
    # Retention opportunities
    result = upgrade_predictors \
        .withColumn("opportunity_score",
                   (col("success_rate_pct") * 0.4 +
                    col("change_count") * 0.0001 * 0.3 +
                    when(col("change_type") == "upgrade", col("avg_content_watched") * 0.3).otherwise(0))) \
        .withColumn("intervention_strategy",
                   when((col("change_pattern") == "Underutilization Downgrade") & (col("avg_completion_rate") > 70), 
                        "Content Recommendations - User engaged but watching less")
                   .when((col("change_pattern") == "Early Upgrade") & (col("avg_days_since_signup") < 15), 
                        "Welcome Offer - Capitalize on early interest")
                   .when((col("change_pattern") == "Usage-Based Upgrade") & (col("avg_devices") > 2), 
                        "Family Plan Promotion - Multiple devices detected")
                   .when(col("success_rate_pct") < 60, "Post-Change Engagement Campaign")
                   .otherwise("Standard Retention Program")) \
        .select("change_type", "change_pattern", "country", "change_count", 
               "success_rate_pct", "opportunity_score", "intervention_strategy") \
        .orderBy(desc("opportunity_score"))
    
    return result