from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    sum, count, avg, countDistinct, desc
)
from lib.Utils import get_spark_session


def load_final_dataframe(spark, final_parquet_path):
    df = spark.read.parquet(final_parquet_path)
    return df

def total_sales_by_category(df):
    return df.groupBy("category") \
             .agg(sum("sales").alias("total_sales"))


def highest_purchase_customer(df):
    return df.groupBy("customer_id", "customer_name") \
             .agg(count("order_id").alias("total_purchases")) \
             .orderBy(desc("total_purchases"))


def average_discount(df):
    return df.agg(avg("discount").alias("average_discount"))


def unique_products_by_region(df):
    return df.groupBy("region") \
             .agg(countDistinct("product_id").alias("unique_products"))


def total_profit_by_state(df):
    return df.groupBy("state") \
             .agg(sum("profit").alias("total_profit"))


def highest_sales_subcategory(df):
    return df.groupBy("sub_category") \
             .agg(sum("sales").alias("total_sales")) \
             .orderBy(desc("total_sales"))


def average_age_by_segment(df):
    return df.groupBy("segment") \
             .agg(avg("age").alias("average_age"))


def shipped_orders_by_mode(df):
    return df.groupBy("ship_mode") \
             .agg(count("order_id").alias("shipped_orders"))


def total_quantity_by_city(df):
    return df.groupBy("city") \
             .agg(sum("quantity").alias("total_quantity"))


def profit_margin_by_segment(df):
    margin_df = df.groupBy("segment") \
                  .agg(
                      sum("profit").alias("total_profit"),
                      sum("sales").alias("total_sales")
                  )
    
    return margin_df.withColumn(
        "profit_margin", margin_df.total_profit / margin_df.total_sales
    ).orderBy(desc("profit_margin"))
