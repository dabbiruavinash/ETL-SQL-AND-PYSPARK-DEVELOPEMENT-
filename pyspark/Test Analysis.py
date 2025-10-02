def ab_test_analysis():
    """Analyze A/B test results for UI features"""
    
    # Create hypothetical A/B test groups
    ab_test_groups = user_profiles_df.alias("up") \
        .join(users_df.alias("u"), "user_id") \
        .withColumn("test_group",
                   when(col("profile_id") % 3 == 0, "Control")
                   .when(col("profile_id") % 3 == 1, "Variant_A")
                   .otherwise("Variant_B")) \
        .withColumn("period",
                   when(col("up.created_date") >= "2024-01-01", "Test_Period")
                   .otherwise("Pre_Test")) \
        .filter(col("up.created_date").between(date_sub(lit("2024-01-01"), 120), lit("2024-03-01")))
    
    # User metrics for test period
    user_metrics = ab_test_groups.alias("ag") \
        .join(viewing_history_df.alias("vh"), "profile_id", "left") \
        .filter(((col("period") == "Pre_Test") & 
                 col("vh.start_time").between(date_sub(lit("2024-01-01"), 60), lit("2024-01-01"))) |
                ((col("period") == "Test_Period") & 
                 col("vh.start_time").between(lit("2024-01-01"), lit("2024-03-01")))) \
        .groupBy("test_group", "period", "ag.user_id") \
        .agg(
            countDistinct("vh.content_id").alias("content_watched"),
            count("vh.view_id").alias("total_views"),
            avg("vh.completion_percentage").alias("completion_rate"),
            countDistinct(when(~col("vh.content_id").isin(
                viewing_history_df.filter(col("start_time") < "2024-01-01")
                .select("content_id").distinct().rdd.flatMap(lambda x: x).collect()
            ), "vh.content_id")).alias("new_content_discovered"),
            avg((unix_timestamp("vh.end_time") - unix_timestamp("vh.start_time")) / 60).alias("avg_session_duration")
        )
    
    # Statistical analysis
    statistical_analysis = user_metrics \
        .groupBy("test_group", "period") \
        .agg(
            countDistinct("user_id").alias("user_count"),
            avg("content_watched").alias("avg_content_watched"),
            avg("total_views").alias("avg_total_views"),
            avg("completion_rate").alias("avg_completion_rate"),
            avg("new_content_discovered").alias("avg_new_content"),
            avg("avg_session_duration").alias("avg_session_duration"),
            stddev("content_watched").alias("std_content_watched"),
            stddev("total_views").alias("std_total_views")
        )
    
    # Control group metrics
    control_metrics = statistical_analysis \
        .filter((col("test_group") == "Control") & (col("period") == "Test_Period")) \
        .select("avg_content_watched", "avg_completion_rate", "avg_new_content") \
        .first()
    
    # Test results with lift calculations
    test_results = statistical_analysis.alias("sa") \
        .filter(col("period") == "Test_Period") \
        .crossJoin(spark.createDataFrame([(
            control_metrics["avg_content_watched"],
            control_metrics["avg_completion_rate"], 
            control_metrics["avg_new_content"]
        )], ["control_avg_content", "control_avg_completion", "control_avg_new"])) \
        .withColumn("content_watched_lift", 
                   ((col("avg_content_watched") / col("control_avg_content")) - 1) * 100) \
        .withColumn("completion_lift",
                   ((col("avg_completion_rate") / col("control_avg_completion")) - 1) * 100) \
        .withColumn("discovery_lift",
                   ((col("avg_new_content") / col("control_avg_new")) - 1) * 100) \
        .withColumn("content_watched_z_score",
                   (col("avg_content_watched") - col("control_avg_content")) / 
                   greatest(col("std_content_watched"), lit(0.001))) \
        .withColumn("statistical_significance",
                   when(abs(col("content_watched_z_score")) > 1.96, "95% Confidence")
                   .when(abs(col("content_watched_z_score")) > 1.645, "90% Confidence")
                   .otherwise("Not Significant")) \
        .withColumn("overall_impact_score",
                   (when(col("content_watched_z_score") > 0, 1).otherwise(-1) * 0.4 +
                    when((col("avg_completion_rate") - col("control_avg_completion")) > 0, 1).otherwise(-1) * 0.3 +
                    when((col("avg_new_content") - col("control_avg_new")) > 0, 1).otherwise(-1) * 0.3)) \
        .withColumn("business_recommendation",
                   when((col("overall_impact_score") > 0.5) & (col("statistical_significance") != "Not Significant"), "Implement Feature")
                   .when((col("overall_impact_score") > 0.2) & (col("statistical_significance") != "Not Significant"), "Consider Implementation")
                   .when(col("overall_impact_score") < -0.2, "Reject Feature")
                   .otherwise("Requires More Testing"))
    
    result = test_results \
        .select("test_group", "user_count", "avg_content_watched", "avg_completion_rate",
               "avg_new_content", "content_watched_lift", "completion_lift", "discovery_lift",
               "statistical_significance", "overall_impact_score", "business_recommendation") \
        .orderBy("test_group")
    
    return result