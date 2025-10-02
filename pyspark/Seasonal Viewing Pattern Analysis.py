def seasonal_viewing_analysis():
    """Analyze viewing patterns across seasons and holidays"""
    
    # Time-based metrics
    time_based_metrics = viewing_history_df.alias("vh") \
        .join(content_df.alias("c"), "content_id") \
        .filter(col("vh.start_time") >= date_sub(current_date(), 730)) \  # 2 years
        .withColumn("view_year", year("vh.start_time")) \
        .withColumn("view_month", month("vh.start_time")) \
        .withColumn("day_of_week", dayofweek("vh.start_time")) \
        .withColumn("season",
                   when(month("vh.start_time").isin(12, 1, 2), "Winter")
                   .when(month("vh.start_time").isin(3, 4, 5), "Spring")
                   .when(month("vh.start_time").isin(6, 7, 8), "Summer")
                   .otherwise("Fall")) \
        .withColumn("holiday_period",
                   when((month("vh.start_time") == 12) & (dayofmonth("vh.start_time").between(20, 31)), "Christmas")
                   .when((month("vh.start_time") == 7) & (dayofmonth("vh.start_time").between(1, 7)), "Summer Holiday")
                   .when((month("vh.start_time") == 3) & (dayofmonth("vh.start_time").between(15, 21)), "Spring Break")
                   .otherwise("Regular")) \
        .withColumn("watch_duration", 
                   (unix_timestamp("vh.end_time") - unix_timestamp("vh.start_time")) / 60) \
        .withColumn("long_session", when(col("watch_duration") > 120, 1).otherwise(0))
    
    # Seasonal trends
    seasonal_trends = time_based_metrics \
        .groupBy("season", "holiday_period", "day_of_week", "genre", "content_type", "maturity_rating") \
        .agg(
            count("*").alias("total_views"),
            countDistinct("profile_id").alias("unique_viewers"),
            avg("completion_percentage").alias("avg_completion"),
            avg("watch_duration").alias("avg_watch_duration"),
            sum("long_session").alias("long_session_views")
        )
    
    # Calculate growth percentages and seasonal indices
    window_spec = Window.partitionBy("genre", "content_type", "day_of_week") \
        .orderBy("season", "holiday_period")
    
    seasonal_analysis = seasonal_trends \
        .withColumn("prev_views", lag("total_views").over(window_spec)) \
        .withColumn("view_growth_percent",
                   when(col("prev_views").isNotNull() & (col("prev_views") > 0),
                        ((col("total_views") - col("prev_views")) / col("prev_views")) * 100)
                   .otherwise(0)) \
        .withColumn("seasonal_index", 
                   col("total_views") / seasonal_trends.agg(avg("total_views")).first()[0])
    
    # Performance classification
    result = seasonal_analysis \
        .withColumn("seasonal_performance",
                   when(col("seasonal_index") > 1.2, "High Seasonal")
                   .when(col("seasonal_index") > 0.8, "Moderate Seasonal")
                   .otherwise("Low Seasonal")) \
        .withColumn("scheduling_opportunity_score",
                   (when(col("seasonal_index") > 1.5, 1).otherwise(0) * 0.4 +
                    when(col("view_growth_percent") > 20, 1).otherwise(0) * 0.3 +
                    when(col("long_session_views") > avg("long_session_views").over(Window.partitionBy()), 1).otherwise(0) * 0.3)) \
        .filter(col("scheduling_opportunity_score") >= 0.5) \
        .select("season", "holiday_period", "day_of_week", "genre", "content_type", 
               "seasonal_performance", "scheduling_opportunity_score", "total_views",
               "avg_completion", "seasonal_index") \
        .orderBy("season", desc("scheduling_opportunity_score"))
    
    return result