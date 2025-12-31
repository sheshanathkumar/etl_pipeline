from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, json, glob, time, random, string, logging
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import count, sum as spark_sum, avg, when, col, round, lit
from db_utils import update_job_status, fetch_jobs_for_transformation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("credit-card-transformation") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


def export_to_excel(transformations_dict, output_path):
    """Export all DataFrames to single Excel file with rounded decimals."""
    logger.info(f"Exporting {len(transformations_dict)} sheets to {output_path}")
    output_path = os.path.join(output_path)
    
    with pd.ExcelWriter( output_path, engine='xlsxwriter') as writer:
        for sheet_name, spark_df in transformations_dict.items():
            # Convert + round decimals to 3 places
            pd_df = spark_df.toPandas()
            pd_df_rounded = pd_df.round(3)  # All decimals → 3 places
            
            # Write to sheet (truncate long names)
            safe_sheet_name = sheet_name[:31]  # Excel limit
            pd_df_rounded.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            logger.info(f"✓ '{safe_sheet_name}' → {len(pd_df)} rows")
    
    logger.info(f"✓ Excel file created: {output_path}")
    return output_path

def transformation_process(raw_path, processed_path):
    logger.info(f"Starting transformation process on file: {raw_path}")
    df = spark.read.option("header", "true").csv(raw_path)
    total_rows = df.count()

    transformations = {}

    transformations["High_Risk_Txns"] = high_risk_transactions(df)
    transformations["Merchant_Performance"] = merchant_performance_analysis(df)
    transformations["Customer_Segments"] = customer_spending_profile(df)
    transformations["Fraud_Alerts"] = fraud_pattern_detection(df)
    transformations["Category_Revenue"] = category_wise_revenue_attribution(df)
    transformations["Reversal_Impact"] = reversal_impact_analysis(df)
    transformations["Bill_Cycle_Anomaly"] = bill_cycle_anomaly_detection(df)
    transformations["Payment_Method"] = payment_method_efficiency(df)
    transformations["Geographic_Risk"] = geographic_risk_scoring(df)
    transformations["Card_Company_Performance"] = card_company_performance_dashboard(df)
    logger.info(f"Transformation process completed for file: {raw_path} with total users: {total_rows}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_path = f"{processed_path}_{timestamp}.xlsx"
    excel_file = export_to_excel(transformations, excel_path)
    
    logger.info(f"Transformation process completed for {raw_path}")
    logger.info(f"Total rows processed: {total_rows}")
    logger.info(f"Excel output: {excel_file}")
    
    return transformations

def high_risk_transactions(df):
    logger.info("Fetching high risk transactions")
    df_highrisk = df.filter(
        (df.txn_amount.cast("decimal") > 50000) &
        (df.txn_status == "Declined") &
        (df.merch_cat.isin("Travel", "Dining"))
    )
    logger.info(f"Found {df_highrisk.count()} high-risk transactions")
    return df_highrisk

def merchant_performance_analysis(df):
    logger.info("Merchant Performance Analysis")
    df_merch_perf = df.groupBy("merch_name", "merch_cat") \
        .agg(
            count("*").alias("total_txns"),
            round(spark_sum("txn_amount"), 2).alias("total_revenue"),
            round(avg("txn_amount"), 2).alias("avg_txn"),
            round((spark_sum(when(df.txn_status == "Approved", 1).otherwise(0)) / count("*")) * 100, 3)
                .alias("approval_rate_pct")
        ).orderBy(F.desc("total_revenue"))
    logger.info(f"Analyzed {df_merch_perf.count()} merchants")
    return df_merch_perf

def customer_spending_profile(df):
    logger.info("Customer Spending Profile...")
    df_customer_profile = df.filter(df.txn_status == "Approved") \
        .groupBy("person_name", "card_company") \
        .agg(
            round(spark_sum("txn_amount"), 2).alias("total_spend"),
            count("*").alias("txn_count"),
            round(avg("txn_amount"), 2).alias("avg_ticket_size")
        ).withColumn("customer_segment", 
                    when(col("total_spend") > 500000, "Premium") 
                    .when(col("total_spend") > 100000, "Regular") 
                    .otherwise("Low-Value"))
    logger.info(f"Generated {df_customer_profile.count()} customer profiles")
    return df_customer_profile

def fraud_pattern_detection(df):
    logger.info("Fraud Pattern Detection")
    df_fraud_alert = df.filter(df.txn_status == "Declined") \
        .groupBy("card_number", df.txn_datetime.cast("timestamp").cast("date").alias("txn_date")) \
        .agg(count("*").alias("decline_count")) \
        .filter(col("decline_count") > 3)
    logger.info(f"Detected {df_fraud_alert.count()} fraud patterns")
    return df_fraud_alert

def category_wise_revenue_attribution(df):
    logger.info("Category-Wise Revenue Attribution")
    df_cat_revenue = df.filter(df.txn_status.isin("Approved", "Pending")) \
        .groupBy("merch_cat", "city") \
        .agg(
            round(spark_sum("txn_amount"), 2).alias("category_revenue"),
            count("*").alias("txn_volume")
        ).orderBy(col("category_revenue").desc())
    logger.info(f"Generated {df_cat_revenue.count()} category breakdowns")
    return df_cat_revenue

def reversal_impact_analysis(df):
    logger.info("Reversal Impact Analysis")
    df_reversal_impact = df.filter(df.txn_status == "Reversed") \
        .groupBy("person_name", "card_company") \
        .agg(
            round(spark_sum("txn_amount"), 2).alias("total_reversed_amount"),
            count("*").alias("reversal_count")
        ).withColumn("reversal_risk_score", (col("total_reversed_amount") / 100000).cast("int"))
    logger.info(f"Analyzed {df_reversal_impact.count()} reversals")
    return df_reversal_impact

def bill_cycle_anomaly_detection(df):
    logger.info("Bill Cycle Anomaly Detection")
    df_bill_cycle = df.groupBy("person_name", "bill_cyc") \
        .agg(
            count("*").alias("txns_per_cycle"),
            round(spark_sum("txn_amount"), 2).alias("spend_per_cycle"),
            round(avg("txn_amount"), 2).alias("avg_per_txn")
        ).filter(col("txns_per_cycle") > 50)
    logger.info(f"Found {df_bill_cycle.count()} bill cycle anomalies")
    return df_bill_cycle

def payment_method_efficiency(df):
    logger.info("Payment Method Efficiency")
    df_approved = df.filter(df.txn_status == "Approved") \
        .groupBy("pay_meth") \
        .agg(
            count("*").alias("successful_txns"),
            round(spark_sum(col("txn_amount").cast("decimal")), 2).alias("processed_volume"),
            round(avg("txn_amount"), 2).alias("avg_txn_size")
        )
    df_all = df.groupBy("pay_meth").agg(count("*").alias("total_txns"))
    df_pay_method = df_approved.join(df_all, "pay_meth", "left") \
        .withColumn("success_rate", 
                   round((col("successful_txns") / col("total_txns") * 100), 2)
                   .cast("decimal(5,2)")) \
        .orderBy(col("processed_volume").desc())
    logger.info(f"Analyzed {df_pay_method.count()} payment methods")
    return df_pay_method

def geographic_risk_scoring(df):
    logger.info("Geographic Risk Scoring")
    df_geo_risk = df.filter(df.txn_status == "Declined") \
        .groupBy("city") \
        .agg(
            count("*").alias("decline_count"),
            round(avg("txn_amount"), 2).alias("avg_declined_amount")
        ).withColumn("risk_score", 
                    round((col("decline_count") * col("avg_declined_amount") / 1000000), 0).cast("int"))
    logger.info(f"Generated {df_geo_risk.count()} geographic risk scores")
    return df_geo_risk

def card_company_performance_dashboard(df):
    logger.info("Card Company Performance Dashboard")
    df_card_perf = df.groupBy("card_company") \
        .agg(
            count("*").alias("total_txns"),
            round(spark_sum(when(df.txn_status == "Approved", df.txn_amount).otherwise(0)), 2)
                .alias("approved_amount"),
            round(spark_sum(when(df.txn_status == "Declined", df.txn_amount).otherwise(0)), 2)
                .alias("declined_amount"),
            round((spark_sum(when(df.txn_status == "Approved", 1).otherwise(0)) / count("*")) * 100, 3)
                .alias("approval_rate_pct")
        ).orderBy(col("approved_amount").desc())
    logger.info(f"Analyzed {df_card_perf.count()} card companies")
    return df_card_perf

def cleanup_temp_files(output_path):
    try :
        folder_path = output_path
        logger.info(f"Cleaning up temporary files in {folder_path}...")

        # removing all non csv files
        for fname in os.listdir(folder_path):
            nonCSV_file = os.path.join(folder_path, fname)
            if os.path.isfile (nonCSV_file) and not fname.endswith('.csv'):
                os.remove(nonCSV_file)
                logger.info(f"Removed temporary file: {nonCSV_file}") 
        
        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
        if not csv_files:
            logger.warning("No CSV files found to rename.")
            return
        if len(csv_files) > 1:
            logger.warning("Multiple CSV files found. Renaming the first one found.")
        
        old_csv = csv_files[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"credit_card_spent_{timestamp}.csv"
        new_path = os.path.join(folder_path, new_name)
        os.rename(old_csv, new_path)
    except Exception as e:
        logger.error(f"Error during cleanup of temporary files: {e}")

if __name__ == "__main__":
    jobs = fetch_jobs_for_transformation(limit=5)
    logger.info(f"Fetched {len(jobs)} jobs for transformation")
    for job in jobs:
        job_id = job["job_id"]
        raw_path = job["raw_path"]

        logger.info(f"Transforming job {job_id} with raw data at {raw_path}")
        processed_path = f"/data/processed/credit_card/job_id={job_id}"
        transformation_process(raw_path, processed_path)
        update_job_status(job_id, "TRANDFORMED", processed_path)
        logger.info(f"Job {job_id} transformed successfully and written to {processed_path}")
        # csv_files = glob.glob(os.path.join(raw_path, "*", "*.csv"))
        # if not csv_files:
        #     logger.error(f"No CSV files found in {raw_path} for job {job_id}")
        #     continue
        # else:
        #     csv_file = csv_files[0]
        #     processed_path = f"/data/processed/credit_card/job_id={job_id}"
        #     transformation_process(csv_file, processed_path)
        #     update_job_status(job_id, "TRANDFORMED", processed_path)
        #     logger.info(f"Job {job_id} transformed successfully and written to {processed_path}")
    logger.info("All transformations completed.")
    
