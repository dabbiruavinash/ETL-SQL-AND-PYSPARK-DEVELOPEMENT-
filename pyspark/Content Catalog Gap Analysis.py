def content_catalog_gap_analysis():
    """Identify gaps in content catalog compared to user demand"""
    
    # User demand analysis
    user_demand_analysis = content_df.alias("c") \
        .join(viewing_history_df.alias("vh"), "content_id", "left") \
        .join(watchlist_df.alias("wl"), "content_id", "left") \
        .filter((col("vh.start_time") >= date_sub(current_date(), 90)) |
                (col("wl.added_date") >= date_sub(current_date(), 90))) \
        .groupBy("c.genre", "c.content_type", "c.maturity_rating") \
        .agg(
            countDistinct("vh.profile_id").alias("current_viewers"),
            count("vh.view_id").alias("current_views"),
            avg("vh.completion_percentage").alias("current_completion_rate"),
            countDistinct("wl.content_id").alias("watchlist_demand")
        )
    
    # Demand growth calculation
    demand_growth = viewing_history_df.alias("vh") \
        .join(content_df.alias("c"), "content_id") \
        .withColumn("period",
                   when(col("vh.start_time") >= date_sub(current_date(), 30), "recent")
                   .when(col("vh.start_time").between(date_sub(current_date(), 60), 
                                                    date_sub(current_date(), 31)), "previous")
                   .otherwise("older")) \
        .filter(col("period").isin(["recent", "previous"])) \
        .groupBy("c.genre", "c.content_type", "period") \
        .agg(count("*").alias("view_count")) \
        .groupBy("genre", "content_type") \
        .pivot("period", ["recent", "previous"]) \
        .agg(first("view_count")) \
        .withColumn("demand_growth_pct",
                   ((col("recent") - col("previous")) / greatest(col("previous"), lit(1))) * 100)
    
    # Catalog coverage analysis
    catalog_coverage = user_demand_analysis.alias("ud") \
        .join(demand_growth.alias("dg"), ["genre", "content_type"], "left") \
        .join(content_df.alias("c"), ["genre", "content_type", "maturity_rating"]) \
        .groupBy("ud.genre", "ud.content_type", "ud.maturity_rating", 
                "ud.current_viewers", "ud.current_views", 
                "ud.current_completion_rate", "ud.watchlist_demand") \
        .agg(
            countDistinct("c.content_id").alias("current_catalog_size"),
            avg("c.imdb_rating").alias("avg_imdb_rating"),
            avg(year(current_date()) - col("c.release_year")).alias("avg_content_age")
        )
    
    # Market coverage ratio calculation
    genre_max_views = catalog_coverage \
        .groupBy("genre") \
        .agg(max("current_views").alias("max_genre_views"))
    
    catalog_coverage_with_ratio = catalog_coverage.alias("cc") \
        .join(genre_max_views.alias("gmv"), "genre") \
        .withColumn("market_coverage_ratio", 
                   (col("current_views") / col("max_genre_views")) * 100)
    
    # Competitor analysis (hypothetical data)
    competitor_data = spark.createDataFrame([
        ("Action", "movie", 150, 6.8, 2018),
        ("Drama", "movie", 200, 7.2, 2017),
        ("Comedy", "movie", 180, 6.5, 2019),
        ("Action", "tv_show", 45, 7.5, 2020),
        ("Drama", "tv_show", 75, 7.8, 2019)
    ], ["genre", "content_type", "competitor_titles", "competitor_avg_rating", "competitor_avg_release_year"])
    
    # Gap analysis
    gap_analysis = catalog_coverage_with_ratio.alias("cc") \
        .join(competitor_data.alias("cd"), ["genre", "content_type"]) \
        .withColumn("title_gap", col("competitor_titles") - col("current_catalog_size")) \
        .withColumn("quality_gap", col("competitor_avg_rating") - col("avg_imdb_rating")) \
        .withColumn("freshness_gap", col("competitor_avg_release_year") - (year(current_date()) - col("avg_content_age"))) \
        .withColumn("acquisition_priority_score",
                   (col("demand_growth_pct") * 0.3 +
                    col("watchlist_demand") * 0.0001 * 0.2 +
                    abs(col("title_gap")) * 0.2 +
                    col("quality_gap") * 0.3))
    
    # Acquisition recommendations
    result = gap_analysis \
        .withColumn("gap_priority",
                   when(col("acquisition_priority_score") > 8, "Critical Gap")
                   .when(col("acquisition_priority_score") > 6, "High Priority")
                   .when(col("acquisition_priority_score") > 4, "Medium Priority")
                   .otherwise("Low Priority")) \
        .withColumn("recommended_action",
                   when((col("title_gap") > 20) & (col("quality_gap") > 0.5), "Major Content Acquisition")
                   .when(col("freshness_gap") > 2, "Focus on New Releases")
                   .when(col("quality_gap") > 0.5, "Quality Content Acquisition")
                   .when(col("title_gap") > 10, "Expand Catalog Volume")
                   .otherwise("Maintain Current Strategy")) \
        .withColumn("budget_allocation",
                   when(col("acquisition_priority_score") > 8, "Allocate 20% of Budget")
                   .when(col("acquisition_priority_score") > 6, "Allocate 15% of Budget")
                   .when(col("acquisition_priority_score") > 4, "Allocate 10% of Budget")
                   .otherwise("Allocate 5% of Budget")) \
        .select("genre", "content_type", "maturity_rating", "current_catalog_size", 
               "competitor_titles", "title_gap", "quality_gap", "freshness_gap",
               "acquisition_priority_score", "gap_priority", "recommended_action", 
               "budget_allocation") \
        .orderBy(desc("acquisition_priority_score"), desc("gap_priority"))
    
    return result