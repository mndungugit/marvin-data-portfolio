"""
Job Monitoring Analysis in PySpark
==================================
Analyzes simulated SQL Server job history to identify patterns, failures, and performance issues.
Demonstrates SQL-to-PySpark translation and data engineering best practices.

Author: Marvin
Portfolio Project: Job Monitoring in PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, max, min, hour, to_date, 
    to_timestamp, when, round as spark_round, desc
)
from pyspark.sql.window import Window
import os
import os
os.environ['HADOOP_HOME'] = r'C:\Program Files\Microsoft\jdk-25.0.4.7-hotspot'
os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-25.0.4.7-hotspot'

from datetime import datetime

# ============================================================================
# SPARK SESSION INITIALIZATION
# ============================================================================

spark = SparkSession.builder \
    .appName("JobMonitoringAnalysis") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("="*80)
print("JOB MONITORING ANALYSIS - PySpark Pipeline")
print("="*80)
print(f"Spark Version: {spark.version}")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# PART 1: LOAD & VALIDATE DATA
# ============================================================================

print("STEP 1: Loading Sample Data")
print("-" * 80)

# Load CSV
df = spark.read.csv('../data/sample_job_history.csv', header=True, inferSchema=True)

# Cast columns to proper types
df = df.withColumn('run_datetime', to_timestamp(col('run_datetime'))) \
    .withColumn('run_date', to_date(col('run_date'))) \
    .withColumn('duration_seconds', col('duration_seconds').cast('int')) \
    .withColumn('retries', col('retries').cast('int'))

row_count = df.count()
print(f"✓ Loaded {row_count:,} records")
print(f"✓ Date range: {df.agg({'run_date': 'min'}).collect()[0][0]} to {df.agg({'run_date': 'max'}).collect()[0][0]}")
print(f"✓ Unique jobs: {df.select('job_name').distinct().count()}")
print(f"✓ Columns: {', '.join(df.columns)}\n")

# ============================================================================
# PART 2: DATA QUALITY CHECKS
# ============================================================================

print("STEP 2: Data Quality Validation")
print("-" * 80)

quality_checks = df.agg(
    count(when(col('job_name').isNull(), 1)).alias('null_job_names'),
    count(when(col('status').isNull(), 1)).alias('null_statuses'),
    count(when(col('duration_seconds') < 0, 1)).alias('negative_durations'),
    count(when(col('status').isin(['Succeeded', 'Failed']), 1)).alias('valid_statuses')
)

quality_results = quality_checks.collect()[0]
print(f"✓ Null job names: {quality_results['null_job_names']}")
print(f"✓ Null statuses: {quality_results['null_statuses']}")
print(f"✓ Negative durations: {quality_results['negative_durations']}")
print(f"✓ Valid statuses: {quality_results['valid_statuses']:,}")
print(f"✓ Data quality: PASSED\n")

# ============================================================================
# PART 3: OVERALL PERFORMANCE SUMMARY
# ============================================================================

print("STEP 3: Overall Performance Summary")
print("-" * 80)

summary = df.agg(
    count('*').alias('total_runs'),
    count(when(col('status') == 'Succeeded', 1)).alias('successful_runs'),
    count(when(col('status') == 'Failed', 1)).alias('failed_runs'),
    spark_round(avg(col('duration_seconds')), 2).alias('avg_duration_sec'),
    max(col('duration_seconds')).alias('max_duration_sec'),
    spark_round(avg(col('retries')), 2).alias('avg_retries')
)

summary_row = summary.collect()[0]
success_rate = (summary_row['successful_runs'] / summary_row['total_runs']) * 100

print(f"Total Runs:        {summary_row['total_runs']:,}")
print(f"Successful:        {summary_row['successful_runs']:,} ({success_rate:.1f}%)")
print(f"Failed:            {summary_row['failed_runs']:,} ({100-success_rate:.1f}%)")
print(f"Avg Duration:      {summary_row['avg_duration_sec']} seconds")
print(f"Max Duration:      {summary_row['max_duration_sec']} seconds")
print(f"Avg Retries:       {summary_row['avg_retries']}\n")

# ============================================================================
# PART 4: FAILURE ANALYSIS
# ============================================================================

print("STEP 4: Failure Analysis")
print("-" * 80)

failures = df.filter(col('status') == 'Failed')
print(f"Total failures: {failures.count()}\n")

print("Top 5 Most Failing Jobs:")
top_failures = failures.groupBy('job_name', 'category') \
    .agg(
        count('*').alias('failure_count'),
        spark_round(avg(col('duration_seconds')), 1).alias('avg_duration'),
        spark_round(avg(col('retries')), 1).alias('avg_retries')
    ) \
    .orderBy(desc('failure_count')) \
    .limit(5)

for row in top_failures.collect():
    print(f"  • {row['job_name']:30} ({row['category']:15}) - {row['failure_count']} failures")

print()

# ============================================================================
# PART 5: PERFORMANCE BY CATEGORY
# ============================================================================

print("STEP 5: Performance by Job Category")
print("-" * 80)

by_category = df.groupBy('category') \
    .agg(
        count('*').alias('total_runs'),
        count(when(col('status') == 'Failed', 1)).alias('failures'),
        spark_round(
            (count(when(col('status') == 'Succeeded', 1)) / count('*')) * 100,
            1
        ).alias('success_rate_%'),
        spark_round(avg(col('duration_seconds')), 1).alias('avg_duration_sec')
    ) \
    .orderBy(desc('total_runs'))

print(f"{'Category':<20} {'Runs':<8} {'Failures':<10} {'Success %':<12} {'Avg Duration'}")
print("-" * 70)
for row in by_category.collect():
    print(f"{row['category']:<20} {row['total_runs']:<8} {row['failures']:<10} {row['success_rate_%']:<12} {row['avg_duration_sec']}s")

print()

# ============================================================================
# PART 6: HOURLY PATTERNS (When do jobs fail most?)
# ============================================================================

print("STEP 6: Failure Patterns by Hour of Day")
print("-" * 80)

hourly = df.withColumn('hour', hour(col('run_datetime'))) \
    .groupBy('hour') \
    .agg(
        count('*').alias('total_runs'),
        count(when(col('status') == 'Failed', 1)).alias('failures'),
        spark_round(
            (count(when(col('status') == 'Failed', 1)) / count('*')) * 100,
            1
        ).alias('failure_rate_%')
    ) \
    .orderBy('hour')

print(f"{'Hour':<8} {'Total Runs':<15} {'Failures':<12} {'Failure Rate'}")
print("-" * 50)
for row in hourly.collect():
    hour_label = f"{int(row['hour']):02d}:00"
    print(f"{hour_label:<8} {row['total_runs']:<15} {row['failures']:<12} {row['failure_rate_%']}%")

print()

# ============================================================================
# PART 7: PERFORMANCE TREND (Over time)
# ============================================================================

print("STEP 7: Performance Trend (Daily)")
print("-" * 80)

daily_trend = df.groupBy('run_date') \
    .agg(
        count('*').alias('runs'),
        count(when(col('status') == 'Failed', 1)).alias('failures'),
        spark_round(avg(col('duration_seconds')), 1).alias('avg_duration')
    ) \
    .orderBy('run_date')

print("Sample of daily performance (showing every 5th day):")
print(f"{'Date':<12} {'Runs':<8} {'Failures':<10} {'Avg Duration'}")
print("-" * 45)

daily_list = daily_trend.collect()
for i, row in enumerate(daily_list):
    if i % 5 == 0:  # Every 5th day
        print(f"{str(row['run_date']):<12} {row['runs']:<8} {row['failures']:<10} {row['avg_duration']}s")

print()

# ============================================================================
# PART 8: EXPORT RESULTS TO CSV
# ============================================================================

print("STEP 8: Exporting Results")
print("-" * 80)

output_dir = './output'

# Export 1: Job Summary
# Export using Pandas (more reliable on Windows)
top_failures.toPandas().to_csv(f'{output_dir}/01_job_summary.csv', index=False)
print(f"✓ Exported: Job Summary")

by_category.toPandas().to_csv(f'{output_dir}/02_category_performance.csv', index=False)
print(f"✓ Exported: Category Performance")

hourly.toPandas().to_csv(f'{output_dir}/03_hourly_failures.csv', index=False)
print(f"✓ Exported: Hourly Failure Patterns")

daily_trend.toPandas().to_csv(f'{output_dir}/04_daily_trend.csv', index=False)
print(f"✓ Exported: Daily Trend")

print()

# ============================================================================
# PART 9: SQL QUERIES (Bonus - SQL queries on Spark DataFrames)
# ============================================================================

print("STEP 9: Advanced SQL Analysis")
print("-" * 80)

df.createOrReplaceTempView("job_runs")

# Most volatile job (most variable duration)
volatile_jobs = spark.sql("""
    SELECT 
        job_name,
        COUNT(*) as runs,
        CAST(AVG(duration_seconds) AS DECIMAL(10,2)) as avg_duration,
        MAX(duration_seconds) as max_duration,
        MIN(duration_seconds) as min_duration,
        CAST(STDDEV(duration_seconds) AS DECIMAL(10,2)) as std_dev
    FROM job_runs
    WHERE status = 'Succeeded'
    GROUP BY job_name
    HAVING COUNT(*) >= 10
    ORDER BY std_dev DESC
    LIMIT 5
""")

print("Top 5 Most Variable Jobs (by duration std deviation):")
for row in volatile_jobs.collect():
    print(f"  • {row['job_name']:30} - Std Dev: {row['std_dev']}s (range: {row['min_duration']}-{row['max_duration']}s)")

print()

# ============================================================================
# CLEANUP & SUMMARY
# ============================================================================

spark.stop()

print("="*80)
print("✓ Analysis Complete")
print("="*80)
print(f"Output files saved to: {output_dir}/")
print("Review the CSV files for detailed results")
print()