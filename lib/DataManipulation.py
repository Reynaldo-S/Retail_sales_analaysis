from pyspark.sql.functions import broadcast,year, month

def handle_missing_values(customers_df, products_df, sales_df):
    customers_df = (
        customers_df
        .fillna({
            "customer_name": "unknown",
            "segment": "unknown",
            "country": "unknown",
            "city": "unknown",
            "state": "unknown",
            "region": "unknown"
        })
        .fillna({"age": 0, "postal_code": 0})
    )

    products_df = (
        products_df
        .fillna({
            "category": "unknown",
            "sub_category": "unknown",
            "product_name": "unknown"
        })
    )

    sales_df = (
        sales_df
        .fillna({
            "ship_mode": "unknown",
            "sales": 0.0,
            "quantity": 0,
            "discount": 0.0,
            "profit": 0.0
        })
    )

    return customers_df, products_df, sales_df

def remove_duplicates(customers_df, products_df, sales_df):

    customers_df = customers_df.dropDuplicates(["customer_id"])
    products_df  = products_df.dropDuplicates(["product_id"])
    sales_df     = sales_df.dropDuplicates(["order_id", "order_line"])

    return customers_df, products_df, sales_df


def join_dataframes(sales_df, customers_df, products_df):
    joined_df = (
        sales_df
        .join(broadcast(customers_df), "customer_id", "left")
        .join(broadcast(products_df), "product_id", "left")
    )
    return joined_df

def add_partition_columns(df):
    df_with_partitions = (
        df
        .withColumn("order_year", year("order_date"))
        .withColumn("order_month", month("order_date"))
    )
    return df_with_partitions

