def competitive_content_benchmarking():
    """Benchmark content performance against industry standards"""
    
    # Hypothetical industry benchmark data
    industry_benchmarks_data = [
        ("Action", "movie", "PG-13", 50000, 75.0, 6.8, 0.15),
        ("Drama", "movie", "R", 45000, 78.0, 7.2, 0.12),
        ("Comedy", "movie", "PG-13", 48000, 72.0, 6.5, 0.14),
        ("Action", "tv_show", "TV-14", 35000, 80.0, 7.5, 0.18),
        ("Drama", "tv_show", "TV-MA", 30000, 82.0, 7.8, 0.16)
    ]
    
    industry_benchmarks = spark.createDataFrame(
        industry_benchmarks_data,
        ["genre", "content_type", "maturity_rating", "industry_avg_viewers", 
         "industry_avg_completion", "industry_avg_rating", "industry_avg_cpv"]
    )
    
    # Platform performance metrics
    platform_performance = content_df.alias("c") \
        .join(viewing_history_df.alias("vh"), "content_id", "left") \
        .filter(col("c.release_year") >= year(current_date()) - 3) \
        .groupBy("c.content_id", "c.title", "c.genre", "c.content_type", 
                "c.maturity_rating", "c.imdb_rating", "c.release_year") \
        .agg(
            countDistinct("vh.profile_id").alias("platform_viewers"),
            count("vh.view_id").alias("platform_views"),
            avg("vh.completion_percentage").alias("platform_completion_rate"),
            sum((unix_timestamp("vh.end_time") - unix_timestamp("vh.start_time")) / 60).alias("total_watch_time")
        ) \
        .filter(col("platform_viewers") >= 1000)
    
    # Competitive analysis
    competitive_analysis = platform_performance.alias("pp") \
        .join(industry_benchmarks.alias("ib"), 
              ["genre", "content_type", "maturity_rating"]) \
        .withColumn("viewer_gap_vs_industry", col("platform_viewers") - col("industry_avg_viewers")) \
        .withColumn("completion_gap_vs_industry", col("platform_completion_rate") - col("industry_avg_completion")) \
        .withColumn("rating_gap_vs_industry", col("imdb_rating") - col("industry_avg_rating")) \
        .withColumn("viewer_performance_tier",
                   when(col("platform_viewers") >= col("industry_avg_viewers") * 1.25, "Industry Leader")
                   .when(col("platform_viewers") >= col("industry_avg_viewers"), "Above Average")
                   .when(col("platform_viewers") >= col("industry_avg_viewers") * 0.7, "Industry Average")
                   .otherwise("Below Average")) \
        .withColumn("retention_performance_tier",
                   when(col("platform_completion_rate") >= col("industry_avg_completion") * 1.1, "Excellent Retention")
                   .when(col("platform_completion_rate") >= col("industry_avg_completion"), "Good Retention")
                   .otherwise("Needs Improvement"))
    
    # Competitive advantages identification
    competitive_advantages = competitive_analysis \
        .withColumn("competitive_score",
                   (when(col("viewer_performance_tier") == "Industry Leader", 1.0)
                    .when(col("viewer_performance_tier") == "Above Average", 0.7)
                    .when(col("viewer_performance_tier") == "Industry Average", 0.5)
                    .otherwise(0.3) * 0.4 +
                    when(col("retention_performance_tier") == "Excellent Retention", 1.0)
                    .when(col("retention_performance_tier") == "Good Retention", 0.7)
                    .otherwise(0.3) * 0.3 +
                    when(col("rating_gap_vs_industry") > 0.5, 1.0)
                    .when(col("rating_gap_vs_industry") > 0, 0.7)
                    .otherwise(0.3) * 0.3)) \
        .withColumn("competitive_advantage",
                   when((col("viewer_gap_vs_industry") > 1000) & (col("completion_gap_vs_industry") > 5), "Dual Strength: Reach & Engagement")
                   .when(col("viewer_gap_vs_industry") > 1000, "Strength: Broad Appeal")
                   .when(col("completion_gap_vs_industry") > 5, "Strength: High Engagement")
                   .when(col("rating_gap_vs_industry") > 0.5, "Strength: Critical Acclaim")
                   .otherwise("No Clear Competitive Advantage"))
    
    # Strategic recommendations
    result = competitive_advantages \
        .withColumn("strategic_recommendation",
                   when(col("competitive_score") > 0.8, "Leverage as Flagship Content")
                   .when((col("competitive_score") > 0.6) & (col("viewer_gap_vs_industry") > 0), "Scale Promotion")
                   .when((col("competitive_score") < 0.4) & (col("completion_gap_vs_industry") < 0), "Improve Content Quality")
                   .when((col("competitive_score") < 0.4) & (col("viewer_gap_vs_industry") < 0), "Enhance Discovery")
                   .otherwise("Maintain Current Strategy")) \
        .select("content_id", "title", "genre", "content_type", "platform_viewers",
               "platform_completion_rate", "imdb_rating", "competitive_score",
               "competitive_advantage", "viewer_performance_tier", "retention_performance_tier",
               "strategic_recommendation") \
        .orderBy(desc("competitive_score"), desc("platform_viewers"))
    
    return result