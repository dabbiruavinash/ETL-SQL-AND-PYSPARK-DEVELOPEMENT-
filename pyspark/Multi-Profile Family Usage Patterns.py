def family_usage_patterns_analysis():
    """Analyze family account usage patterns"""
    
    # Family accounts identification
    family_accounts = users_df.alias("u") \
        .join(user_profiles_df.alias("up"), "user_id") \
        .filter(col("subscription_type") == "family") \
        .groupBy("u.user_id", "u.subscription_type", "u.country", "u.signup_date") \
        .agg(
            countDistinct("up.profile_id").alias("total_profiles"),
            sum(when(col("is_kid_profile") == True, 1).otherwise(0)).alias("kid_profiles")
        ) \
        .withColumn("account_age_days", datediff(current_date(), "signup_date")) \
        .filter(col("total_profiles") >= 2)
    
    # Individual profile usage metrics
    profile_usage_metrics = family_accounts.alias("fa") \
        .join(user_profiles_df.alias("up"), col("fa.user_id") == col("up.user_id")) \
        .join(viewing_history_df.alias("vh"), "profile_id", "left") \
        .join(content_df.alias("c"), "content_id", "left") \
        .filter(col("vh.start_time") >= date_sub(current_date(), 90)) \
        .groupBy("fa.user_id", "up.profile_id", "up.profile_name", "up.is_kid_profile") \
        .agg(
            countDistinct("vh.content_id").alias("profile_content_watched"),
            count("vh.view_id").alias("profile_total_views"),
            avg("vh.completion_percentage").alias("profile_completion_rate"),
            countDistinct(when(col("c.content_type") == "movie", "c.content_id")).alias("movies_watched"),
            countDistinct(when(col("c.content_type") == "tv_show", "c.content_id")).alias("tv_shows_watched"),
            avg(when(hour("vh.start_time").between(16, 22), 1).otherwise(0)).alias("prime_time_ratio"),
            avg(when(hour("vh.start_time").between(22, 6), 1).otherwise(0)).alias("late_night_ratio"),
            countDistinct("vh.device_type").alias("devices_used")
        )
    
    # Family content overlap analysis
    family_content_overlap = family_accounts.alias("fa") \
        .join(user_profiles_df.alias("up"), col("fa.user_id") == col("up.user_id")) \
        .join(viewing_history_df.alias("vh"), "profile_id") \
        .join(content_df.alias("c"), "content_id") \
        .filter(col("vh.start_time") >= date_sub(current_date(), 90)) \
        .groupBy("fa.user_id") \
        .agg(
            countDistinct("vh.content_id").alias("family_total_content"),
            countDistinct("up.profile_id").alias("active_profiles"),
            countDistinct("c.genre").alias("genres_watched")
        )
    
    # Shared content calculation
    shared_content = viewing_history_df.alias("vh1") \
        .join(user_profiles_df.alias("up1"), "profile_id") \
        .join(viewing_history_df.alias("vh2"), 
              (col("vh1.content_id") == col("vh2.content_id")) &
              (col("up1.user_id") == user_profiles_df.alias("up2").select("user_id")) &
              (col("vh1.profile_id") != col("vh2.profile_id"))) \
        .groupBy("up1.user_id") \
        .agg(countDistinct("vh1.content_id").alias("shared_content"))
    
    # Same-day family views
    same_day_family_views = viewing_history_df.alias("vh") \
        .join(user_profiles_df.alias("up"), "profile_id") \
        .groupBy("up.user_id", "vh.content_id", date_format("vh.start_time", "yyyy-MM-dd").alias("view_date")) \
        .agg(countDistinct("profile_id").alias("profiles_watched")) \
        .filter(col("profiles_watched") > 1) \
        .groupBy("user_id") \
        .agg(countDistinct("content_id").alias("same_day_family_views"))
    
    # Combine all family metrics
    family_engagement_analysis = family_accounts.alias("fa") \
        .join(family_content_overlap.alias("fco"), "user_id") \
        .join(shared_content.alias("sc"), "user_id", "left") \
        .join(same_day_family_views.alias("sdfv"), "user_id", "left") \
        .withColumn("family_engagement_score",
                   (col("active_profiles") / col("total_profiles") * 0.3 +
                    coalesce(col("shared_content"), lit(0)) / greatest(col("family_total_content"), lit(1)) * 0.4 +
                    coalesce(col("same_day_family_views"), lit(0)) / greatest(col("family_total_content"), lit(1)) * 0.3)) \
        .withColumn("usage_inequality", lit(50))  # Simplified - would calculate STDDEV in real scenario
    
    # Profile usage distribution analysis
    profile_usage_std = profile_usage_metrics.alias("pum") \
        .groupBy("user_id") \
        .agg(stddev("profile_total_views").alias("usage_stddev"))
    
    # Final result with classifications
    result = family_engagement_analysis.alias("fea") \
        .join(profile_usage_std.alias("pus"), "user_id") \
        .withColumn("family_type",
                   when((col("family_engagement_score") > 0.7) & (col("usage_stddev") < 50), "Highly Engaged Family")
                   .when((col("family_engagement_score") > 0.5) & (col("kid_profiles") > 0), "Family with Kids")
                   .when(col("usage_stddev") > 100, "Dominant User Account")
                   .when(col("family_engagement_score") < 0.3, "Low Engagement Family")
                   .otherwise("Balanced Family Usage")) \
        .withColumn("retention_risk",
                   when((col("family_engagement_score") < 0.2) & (col("account_age_days") > 180), "High Churn Risk")
                   .when((col("family_engagement_score") < 0.4) & (col("usage_stddev") > 150), "Medium Churn Risk")
                   .otherwise("Low Churn Risk")) \
        .select("user_id", "subscription_type", "country", "total_profiles", "kid_profiles",
               "family_engagement_score", "usage_stddev", "shared_content", "same_day_family_views",
               "family_type", "retention_risk") \
        .orderBy(desc("family_engagement_score"))
    
    return result