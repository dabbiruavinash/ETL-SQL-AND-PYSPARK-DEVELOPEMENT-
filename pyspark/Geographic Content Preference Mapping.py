def geographic_content_preferences():
    """Analyze regional content preferences across countries"""
    
    # Countries of interest
    target_countries = ["US", "UK", "CA", "DE", "FR", "JP", "KR", "BR", "MX", "IN"]
    
    # Regional viewership analysis
    regional_viewership = viewing_history_df.alias("vh") \
        .join(user_profiles_df.alias("up"), "profile_id") \
        .join(users_df.alias("u"), "user_id") \
        .join(content_df.alias("c"), "content_id") \
        .filter(col("u.country").isin(target_countries)) \
        .filter(col("vh.start_time") >= date_sub(current_date(), 365)) \
        .withColumn("quarter", quarter("vh.start_time")) \
        .withColumn("time_of_day",
                   when(hour("vh.start_time").between(6, 12), "Morning")
                   .when(hour("vh.start_time").between(12, 18), "Afternoon")
                   .when(hour("vh.start_time").between(18, 23), "Evening")
                   .otherwise("Late Night")) \
        .withColumn("content_origin", 
                   when(col("c.origin_country") == col("u.country"), "Domestic")
                   .when(col("c.origin_country").isin(["US", "UK", "CA"]), "Western")
                   .when(col("c.origin_country").isin(["JP", "KR", "CN"]), "Asian")
                   .otherwise("International"))  # Assuming origin_country exists
    
    # Regional preferences aggregation
    regional_preferences = regional_viewership \
        .groupBy("u.country", "c.genre", "c.content_type", "content_origin") \
        .agg(
            countDistinct("vh.profile_id").alias("total_viewers"),
            count("vh.view_id").alias("total_views"),
            avg("vh.completion_percentage").alias("avg_completion")
        )
    
    # Calculate market share within each country
    country_totals = regional_preferences \
        .groupBy("country") \
        .agg(sum("total_viewers").alias("country_total_viewers"))
    
    regional_preferences_with_share = regional_preferences.alias("rp") \
        .join(country_totals.alias("ct"), "country") \
        .withColumn("market_share_pct", 
                   (col("total_viewers") / col("country_total_viewers")) * 100)
    
    # Quarterly growth calculation
    quarterly_viewership = regional_viewership \
        .groupBy("country", "genre", "content_type", "quarter") \
        .agg(countDistinct("profile_id").alias("quarterly_viewers"))
    
    quarterly_growth = quarterly_viewership.alias("q1") \
        .join(quarterly_viewership.alias("q2"),
              (col("q1.country") == col("q2.country")) &
              (col("q1.genre") == col("q2.genre")) &
              (col("q1.content_type") == col("q2.content_type")) &
              (col("q1.quarter") == 4) & (col("q2.quarter") == 1)) \
        .withColumn("quarterly_growth",
                   ((col("q1.quarterly_viewers") - col("q2.quarterly_viewers")) / 
                    greatest(col("q2.quarterly_viewers"), lit(1))) * 100)
    
    # Cross-country analysis for localization opportunities
    cross_country_analysis = regional_preferences_with_share.alias("rp1") \
        .join(regional_preferences_with_share.alias("rp2"),
              (col("rp1.genre") == col("rp2.genre")) &
              (col("rp1.content_type") == col("rp2.content_type")) &
              (col("rp1.country") != col("rp2.country"))) \
        .filter((col("rp1.total_viewers") >= 1000) & (col("rp2.total_viewers") >= 1000)) \
        .withColumn("preference_similarity",
                   (abs(col("rp1.market_share_pct") - col("rp2.market_share_pct")) * -0.4 +
                    abs(col("rp1.avg_completion") - col("rp2.avg_completion")) * -0.3 +
                    when(col("rp1.content_origin") == col("rp2.content_origin"), 0.3).otherwise(0))) \
        .withColumn("content_gap", col("rp2.market_share_pct") - col("rp1.market_share_pct"))
    
    # Localization opportunities identification
    localization_opportunities = cross_country_analysis \
        .filter(col("content_gap") > 2) \
        .withColumn("localization_priority",
                   when((col("preference_similarity") > 0.8) & (col("content_gap") > 5), "High Priority")
                   .when((col("preference_similarity") > 0.6) & (col("content_gap") > 3), "Medium Priority")
                   .otherwise("Low Priority")) \
        .withColumn("estimated_revenue_impact",
                   col("content_gap") * 1000 * 0.01 * 15.99)  # Simplified calculation
    
    result = localization_opportunities \
        .select("rp1.country", "rp2.country", "rp1.genre", "rp1.content_type",
               "preference_similarity", "content_gap", "localization_priority",
               "estimated_revenue_impact") \
        .filter(col("localization_priority") != "Low Priority") \
        .orderBy(desc("localization_priority"), desc("estimated_revenue_impact"))
    
    return result