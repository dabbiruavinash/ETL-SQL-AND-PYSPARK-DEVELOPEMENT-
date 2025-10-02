def content_affinity_analysis():
    """Identify content genre relationships and user cross-genre preferences"""
    
    # User genre affinity
    user_genre_affinity = viewing_history_df.alias("vh") \
        .join(user_profiles_df.alias("up"), "profile_id") \
        .join(content_df.alias("c"), "content_id") \
        .filter(col("vh.start_time") >= date_sub(current_date(), 90)) \
        .groupBy("up.profile_id", "c.genre") \
        .agg(
            countDistinct("vh.content_id").alias("genre_view_count"),
            sum("vh.completion_percentage").alias("total_completion"),
            avg("vh.completion_percentage").alias("avg_completion"),
            countDistinct(date_format("vh.start_time", "yyyy-MM-dd")).alias("active_days")
        ) \
        .withColumn("genre_affinity_score",
                   (col("genre_view_count") * 0.4 +
                    col("avg_completion") * 0.3 +
                    col("active_days") * 0.3)) \
        .filter(col("genre_view_count") >= 3)
    
    # Genre combinations within users
    genre_combinations = user_genre_affinity.alias("uga1") \
        .join(user_genre_affinity.alias("uga2"), 
              (col("uga1.profile_id") == col("uga2.profile_id")) & 
              (col("uga1.genre") != col("uga2.genre"))) \
        .filter((col("uga1.genre_affinity_score") >= 0.6) & 
                (col("uga2.genre_affinity_score") >= 0.4)) \
        .withColumn("compatibility_score", 
                   col("uga1.genre_affinity_score") * col("uga2.genre_affinity_score"))
    
    # Co-viewing days calculation
    co_viewing_days = viewing_history_df.alias("vh1") \
        .join(content_df.alias("c1"), "content_id") \
        .join(viewing_history_df.alias("vh2"), 
              (col("vh1.profile_id") == col("vh2.profile_id")) &
              (date_format("vh1.start_time", "yyyy-MM-dd") == date_format("vh2.start_time", "yyyy-MM-dd"))) \
        .join(content_df.alias("c2"), col("vh2.content_id") == col("c2.content_id")) \
        .filter(col("c1.genre") != col("c2.genre")) \
        .select("vh1.profile_id", "c1.genre", "c2.genre", 
                date_format("vh1.start_time", "yyyy-MM-dd").alias("view_date")) \
        .distinct() \
        .groupBy("profile_id", "genre", "c2.genre") \
        .agg(count("*").alias("co_viewing_days"))
    
    # Genre network analysis
    genre_network = genre_combinations.alias("gc") \
        .join(co_viewing_days.alias("cvd"),
              (col("gc.profile_id") == col("cvd.profile_id")) &
              (col("gc.uga1.genre") == col("cvd.genre")) &
              (col("gc.uga2.genre") == col("cvd.c2.genre")), "left") \
        .groupBy(col("uga1.genre").alias("primary_genre"), 
                col("uga2.genre").alias("secondary_genre")) \
        .agg(
            countDistinct("gc.profile_id").alias("user_count"),
            avg("compatibility_score").alias("avg_compatibility"),
            avg(coalesce("co_viewing_days", lit(0))).alias("avg_co_viewing_days")
        ) \
        .withColumn("relationship_strength",
                   (col("user_count") * 0.5 +
                    col("avg_compatibility") * 0.3 +
                    col("avg_co_viewing_days") * 0.2)) \
        .filter(col("user_count") >= 10)
    
    # Content gap analysis
    content_gap = content_df.alias("c") \
        .groupBy("genre") \
        .agg(count("*").alias("content_count"))
    
    result = genre_network.alias("gn") \
        .join(content_gap.alias("cg1"), col("gn.primary_genre") == col("cg1.genre")) \
        .join(content_gap.alias("cg2"), col("gn.secondary_genre") == col("cg2.genre")) \
        .withColumn("opportunity_score",
                   col("relationship_strength") * 
                   (col("cg2.content_count") - col("cg1.content_count")) * 0.01) \
        .select("primary_genre", "secondary_genre", "user_count", "avg_compatibility",
               "avg_co_viewing_days", "relationship_strength", "opportunity_score") \
        .orderBy(desc("relationship_strength"), desc("opportunity_score"))
    
    return result