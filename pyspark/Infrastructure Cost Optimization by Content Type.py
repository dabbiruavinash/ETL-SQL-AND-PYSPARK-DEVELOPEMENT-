def infrastructure_cost_optimization():
    """Analyze infrastructure costs by content type and optimize spending"""
    
    # Hypothetical infrastructure costs table
    infrastructure_costs_schema = StructType([
        StructField("content_id", IntegerType(), True),
        StructField("storage_cost_monthly", DoubleType(), True),
        StructField("cdn_cost_monthly", DoubleType(), True),
        StructField("transcoding_cost_monthly", DoubleType(), True),
        StructField("total_cost_monthly", DoubleType(), True),
        StructField("cost_per_gb_streamed", DoubleType(), True),
        StructField("month", DateType(), True)
    ])
    
    infrastructure_costs_df = spark.table("infrastructure_costs") \
        .filter(col("month") == date_format(current_date(), "yyyy-MM-01"))
    
    # Streaming metrics by content
    streaming_metrics = viewing_history_df.alias("vh") \
        .join(content_df.alias("c"), "content_id") \
        .filter(col("vh.start_time") >= date_sub(current_date(), 90)) \
        .groupBy("c.content_id", "c.content_type", "c.genre", "c.duration_minutes", "c.quality_tier") \
        .agg(
            count("vh.view_id").alias("total_streams"),
            sum((unix_timestamp("vh.end_time") - unix_timestamp("vh.start_time"))).alias("total_stream_seconds"),
            avg((unix_timestamp("vh.end_time") - unix_timestamp("vh.start_time"))).alias("avg_stream_duration"),
            countDistinct("vh.device_type").alias("device_types"),
            countDistinct(users_df.alias("u").join(user_profiles_df.alias("up"), "user_id")
                         .filter(col("up.profile_id") == col("vh.profile_id"))
                         .select("country")).alias("countries_streamed")
        ) \
        .filter(col("total_streams") >= 100)
    
    # Quality distribution (simplified)
    quality_distribution = viewing_history_df.alias("vh") \
        .groupBy("content_id", "quality") \
        .agg(count("*").alias("quality_streams")) \
        .groupBy("content_id") \
        .pivot("quality", ["4K", "HD", "SD"]) \
        .agg(first("quality_streams"))
    
    # Content cost analysis
    content_cost_analysis = streaming_metrics.alias("sm") \
        .join(infrastructure_costs_df.alias("ic"), "content_id") \
        .join(quality_distribution.alias("qd"), "content_id", "left") \
        .withColumn("streams_per_dollar", col("total_streams") / greatest(col("total_cost_monthly"), lit(1))) \
        .withColumn("seconds_per_dollar", col("total_stream_seconds") / greatest(col("total_cost_monthly"), lit(1))) \
        .withColumn("cost_per_stream", col("total_cost_monthly") / greatest(col("total_streams"), lit(1))) \
        .withColumn("weighted_cdn_cost",
                   when(col("4K").isNotNull(), col("cdn_cost_monthly") * 1.5)
                   .when(col("HD").isNotNull(), col("cdn_cost_monthly") * 1.0)
                   .otherwise(col("cdn_cost_monthly") * 0.5)) \
        .withColumn("storage_cost_per_stream", 
                   col("storage_cost_monthly") / greatest(col("total_streams"), lit(1)))
    
    # Cost optimization opportunities
    cost_optimization_opportunities = content_cost_analysis \
        .groupBy("content_type", "genre", "quality_tier") \
        .agg(
            count("*").alias("content_count"),
            sum("total_streams").alias("total_streams"),
            sum("total_cost_monthly").alias("total_monthly_cost"),
            avg("cost_per_stream").alias("avg_cost_per_stream"),
            avg("streams_per_dollar").alias("avg_streams_per_dollar"),
            sum("storage_cost_monthly").alias("total_storage_cost"),
            sum("cdn_cost_monthly").alias("total_cdn_cost"),
            sum("transcoding_cost_monthly").alias("total_transcoding_cost")
        ) \
        .filter(col("content_count") >= 10)
    
    # Percentile calculations for benchmarks
    window_spec = Window.partitionBy("content_type", "genre")
    cost_percentiles = content_cost_analysis \
        .withColumn("cost_25p", percentile_approx("cost_per_stream", 0.25).over(window_spec)) \
        .withColumn("cost_75p", percentile_approx("cost_per_stream", 0.75).over(window_spec))
    
    cost_benchmarks = cost_percentiles \
        .groupBy("content_type", "genre") \
        .agg(
            first("cost_25p").alias("cost_25p"),
            first("cost_75p").alias("cost_75p")
        )
    
    # Optimization recommendations
    optimization_recommendations = cost_optimization_opportunities.alias("coo") \
        .join(cost_benchmarks.alias("cb"), ["content_type", "genre"]) \
        .withColumn("cost_efficiency_gap", col("avg_cost_per_stream") - col("cost_25p")) \
        .withColumn("optimization_priority",
                   when(col("avg_cost_per_stream") > col("cost_75p"), "High Priority")
                   .when(col("avg_cost_per_stream") > col("cost_25p"), "Medium Priority")
                   .otherwise("Low Priority")) \
        .withColumn("cost_optimization_area",
                   when((col("total_storage_cost") > col("total_cdn_cost")) & (col("total_streams") < 1000), 
                        "Consider Archive: High storage cost for low streams")
                   .when((col("total_cdn_cost") > col("total_storage_cost") * 2) & (col("quality_tier") == "4K"), 
                        "Optimize UHD Delivery: High CDN costs")
                   .when(col("avg_streams_per_dollar") < 10, 
                        "Review Encoding: Low streams per infrastructure dollar")
                   .otherwise("Efficient: Monitor for changes")) \
        .withColumn("potential_monthly_savings",
                   when(col("optimization_priority") == "High Priority", col("total_monthly_cost") * 0.25)
                   .when(col("optimization_priority") == "Medium Priority", col("total_monthly_cost") * 0.15)
                   .otherwise(col("total_monthly_cost") * 0.05)) \
        .withColumn("implementation_complexity",
                   when((col("content_type") == "movie") & (col("quality_tier") == "4K"), "High Complexity")
                   .when(col("genre").isin(["Documentary", "Standup"]), "Low Complexity")
                   .otherwise("Medium Complexity"))
    
    # Final result with ROI calculations
    result = optimization_recommendations \
        .withColumn("estimated_roi_months",
                   (col("potential_monthly_savings") * 12) / 
                   when(col("implementation_complexity") == "High Complexity", 50000)
                   .when(col("implementation_complexity") == "Medium Complexity", 25000)
                   .otherwise(10000)) \
        .withColumn("implementation_timeline",
                   when((col("optimization_priority") == "High Priority") & 
                        (col("implementation_complexity") == "Low Complexity"), "Implement Immediately")
                   .when(col("optimization_priority") == "High Priority", "Plan Q1 Implementation")
                   .when((col("optimization_priority") == "Medium Priority") & 
                         (col("implementation_complexity") == "Low Complexity"), "Plan Q2 Implementation")
                   .otherwise("Monitor and Re-evaluate Next Quarter")) \
        .select("content_type", "genre", "quality_tier", "total_monthly_cost", 
               "avg_cost_per_stream", "optimization_priority", "cost_optimization_area",
               "potential_monthly_savings", "implementation_complexity", "estimated_roi_months",
               "implementation_timeline", "content_count") \
        .orderBy(desc("potential_monthly_savings"), desc("optimization_priority"))
    
    return result