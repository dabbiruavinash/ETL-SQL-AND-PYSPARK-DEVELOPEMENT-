def content_production_roi_analysis():
    """Calculate ROI for original content productions"""
    
    # Hypothetical production costs table
    production_costs_schema = StructType([
        StructField("content_id", IntegerType(), True),
        StructField("production_budget", DoubleType(), True),
        StructField("marketing_budget", DoubleType(), True),
        StructField("production_country", StringType(), True),
        StructField("production_year", IntegerType(), True),
        StructField("content_category", StringType(), True),
        StructField("genre_focus", StringType(), True)
    ])
    
    production_costs_df = spark.table("production_costs")
    
    # Content performance metrics
    content_performance = production_costs_df.alias("pc") \
        .join(content_df.alias("c"), "content_id") \
        .join(viewing_history_df.alias("vh"), "content_id", "left") \
        .groupBy("pc.content_id", "c.title", "c.genre", "c.content_type", "c.imdb_rating") \
        .agg(
            countDistinct("vh.profile_id").alias("total_viewers"),
            count("vh.view_id").alias("total_views"),
            avg("vh.completion_percentage").alias("avg_completion"),
            sum((unix_timestamp("vh.end_time") - unix_timestamp("vh.start_time")) / 60).alias("total_watch_minutes")
        )
    
    # Attributed subscriptions (users who joined around content release)
    attributed_subscriptions = users_df.alias("u") \
        .join(viewing_history_df.alias("vh"), 
              (col("u.user_id") == user_profiles_df.alias("up").select("user_id")) &
              (col("vh.profile_id") == col("up.profile_id"))) \
        .join(content_df.alias("c"), "content_id") \
        .filter(col("u.signup_date").between(col("c.added_date"), date_add(col("c.added_date"), 30))) \
        .filter(col("vh.start_time").between(col("u.signup_date"), date_add(col("u.signup_date"), 7))) \
        .groupBy("c.content_id") \
        .agg(countDistinct("u.user_id").alias("attributed_subscriptions"))
    
    # Financial metrics calculation
    financial_metrics = content_performance.alias("cp") \
        .join(production_costs_df.alias("pc"), "content_id") \
        .join(attributed_subscriptions.alias("as"), "content_id", "left") \
        .withColumn("total_cost", col("production_budget") + col("marketing_budget")) \
        .withColumn("estimated_annual_revenue", 
                   coalesce(col("attributed_subscriptions"), lit(0)) * 15.99 * 12) \
        .withColumn("engagement_revenue", col("total_watch_minutes") * 0.01)  # $0.01 per minute
    
    # ROI analysis
    roi_analysis = financial_metrics \
        .withColumn("net_profit", 
                   col("estimated_annual_revenue") + col("engagement_revenue") - col("total_cost")) \
        .withColumn("roi_percentage",
                   when(col("total_cost") > 0,
                        ((col("estimated_annual_revenue") + col("engagement_revenue") - col("total_cost")) / 
                         col("total_cost")) * 100)
                   .otherwise(0)) \
        .withColumn("success_category",
                   when(col("roi_percentage") > 100, "High Success")
                   .when(col("roi_percentage") > 50, "Moderate Success")
                   .when(col("roi_percentage") > 0, "Break Even")
                   .otherwise("Underperforming"))
    
    # Production patterns analysis
    production_patterns = roi_analysis \
        .groupBy("genre", "content_type", "content_category", "production_country") \
        .agg(
            count("*").alias("productions_count"),
            avg("roi_percentage").alias("avg_roi"),
            avg("imdb_rating").alias("avg_imdb_rating"),
            avg("avg_completion").alias("avg_completion_rate"),
            (sum(when(col("success_category") == "High Success", 1).otherwise(0)) * 100.0 / 
             count("*")).alias("high_success_rate"),
            stddev("roi_percentage").alias("roi_volatility"),
            avg(col("total_viewers") / col("total_cost") * 1000).alias("viewers_per_thousand_dollars")
        ) \
        .filter(col("productions_count") >= 5)
    
    # Investment recommendations
    result = production_patterns \
        .withColumn("investment_recommendation",
                   when((col("avg_roi") > 100) & (col("roi_volatility") < 50), "Increase Investment")
                   .when((col("avg_roi") > 50) & (col("high_success_rate") > 30), "Maintain Investment")
                   .when((col("avg_roi") < 0) & (col("high_success_rate") < 10), "Reduce Investment")
                   .otherwise("Further Analysis Required")) \
        .select("genre", "content_type", "content_category", "production_country",
               "productions_count", "avg_roi", "high_success_rate", "roi_volatility",
               "viewers_per_thousand_dollars", "investment_recommendation") \
        .orderBy(desc("avg_roi"))
    
    return result