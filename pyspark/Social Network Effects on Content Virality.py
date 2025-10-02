def social_network_virality_analysis():
    """Analyze how content spreads through user networks"""
    
    # Create hypothetical user network (in real scenario, this would come from social features)
    user_network = users_df.alias("u1") \
        .crossJoin(users_df.alias("u2")) \
        .filter(col("u1.user_id") != col("u2.user_id")) \
        .join(user_profiles_df.alias("up1"), col("u1.user_id") == col("up1.user_id")) \
        .join(user_profiles_df.alias("up2"), col("u2.user_id") == col("up2.user_id")) \
        .join(viewing_history_df.alias("vh1"), col("up1.profile_id") == col("vh1.profile_id"), "left") \
        .join(viewing_history_df.alias("vh2"), col("up2.profile_id") == col("vh2.profile_id"), "left") \
        .join(content_df.alias("c1"), col("vh1.content_id") == col("c1.content_id"), "left") \
        .join(content_df.alias("c2"), col("vh2.content_id") == col("c2.content_id"), "left") \
        .filter(col("u1.signup_date") >= date_sub(current_date(), 365)) \
        .filter(col("u2.signup_date") >= date_sub(current_date(), 365)) \
        .groupBy("u1.user_id", "u2.user_id", "u1.country", "u2.country") \
        .agg(
            countDistinct(when(col("vh1.content_id") == col("vh2.content_id"), col("vh1.content_id"))).alias("shared_content"),
            countDistinct(when(col("c1.genre") == col("c2.genre"), col("c1.genre"))).alias("shared_genres"),
            count("*").alias("interaction_count")
        ) \
        .withColumn("connection_strength",
                   (col("shared_content") * 0.5 +
                    col("shared_genres") * 0.3 +
                    when(col("u1.country") == col("u2.country"), 0.2).otherwise(0))) \
        .filter((col("connection_strength") > 0.3) & (col("shared_content") >= 2)) \
        .select("u1.user_id", "u2.user_id", "connection_strength")
    
    # Content adoption curve
    content_adoption_curve = viewing_history_df.alias("vh") \
        .join(content_df.alias("c"), "content_id") \
        .join(user_profiles_df.alias("up"), "profile_id") \
        .join(user_network.alias("un"), col("up.user_id") == col("un.user_id"), "left") \
        .filter(col("vh.start_time") >= date_sub(current_date(), 90)) \
        .groupBy("c.content_id", "c.title", "c.genre", "c.content_type") \
        .agg(
            countDistinct("vh.profile_id").alias("total_viewers"),
            countDistinct(date_format("vh.start_time", "yyyy-MM-dd")).alias("adoption_days"),
            min("vh.start_time").alias("first_view"),
            max("vh.start_time").alias("last_view"),
            countDistinct("un.u2.user_id").alias("network_reach"),
            avg("un.connection_strength").alias("avg_connection_strength")
        ) \
        .withColumn("virality_coefficient", 
                   col("network_reach") / greatest(col("total_viewers"), lit(1))) \
        .filter(col("total_viewers") >= 10)
    
    # Early adopters analysis
    early_adopters = viewing_history_df.alias("vh") \
        .join(content_adoption_curve.alias("cac"), "content_id") \
        .filter(col("vh.start_time") <= date_add(col("cac.first_view"), 7)) \
        .groupBy("cac.content_id") \
        .agg(countDistinct("profile_id").alias("early_adopters"))
    
    # Network analysis
    network_analysis = content_adoption_curve.alias("cac") \
        .join(early_adopters.alias("ea"), "content_id", "left") \
        .join(user_network.alias("un"), 
              col("cac.content_id").isin(
                  viewing_history_df.join(user_profiles_df, "profile_id")
                  .filter(col("user_id") == col("un.user_id"))
                  .select("content_id").distinct().rdd.flatMap(lambda x: x).collect()
              ), "left") \
        .groupBy("cac.content_id", "cac.title", "cac.genre", "cac.content_type",
                "cac.total_viewers", "cac.virality_coefficient") \
        .agg(
            avg("ea.early_adopters").alias("early_adopters"),
            countDistinct("un.u2.user_id").alias("total_connections"),
            avg(viewing_history_df.join(user_ratings_df, ["profile_id", "content_id"])
                .select("rating").rdd.flatMap(lambda x: x).collect()).alias("avg_rating"),
            avg("cac.avg_connection_strength").alias("avg_connection_strength")
        )
    
    # Virality classification
    result = network_analysis \
        .withColumn("virality_category",
                   when(col("virality_coefficient") > 2.0, "Highly Viral")
                   .when(col("virality_coefficient") > 1.0, "Viral")
                   .when(col("virality_coefficient") > 0.5, "Moderate Spread")
                   .otherwise("Limited Spread")) \
        .withColumn("success_score",
                   (col("virality_coefficient") * 0.3 +
                    (col("early_adopters") / greatest(col("total_viewers"), lit(1))) * 0.2 +
                    coalesce(col("avg_rating"), lit(0)) / 5 * 0.3 +
                    col("avg_connection_strength") * 0.2)) \
        .filter(col("total_viewers") >= 50) \
        .select("content_id", "title", "genre", "content_type", "total_viewers", 
               "early_adopters", "network_reach", "virality_coefficient", "avg_rating",
               "virality_category", "success_score") \
        .orderBy(desc("virality_coefficient"), desc("success_score"))
    
    return result