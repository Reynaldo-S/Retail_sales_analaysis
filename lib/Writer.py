def write_partitioned_parquet(df, output_path):
    (
        df.write
        .mode("overwrite")
        .partitionBy("order_year", "order_month", "region")
        .parquet(output_path)
    )
