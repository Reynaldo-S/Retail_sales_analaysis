import sys
from lib.DataReader import read_customers, read_products, read_sales
from lib.Utils import get_spark_session
from lib.ConfigReader import get_app_config
from lib.DataManipulation import handle_missing_values, remove_duplicates, join_dataframes, add_partition_columns
from lib.Writer import write_partitioned_parquet
from lib.Analytics import *



if __name__ == '__main__':

    if len(sys.argv) < 2:
        print(" Please specify environment (LOCAL / DEV / PROD)")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print(" Creating Spark Session...")
    spark = get_spark_session(job_run_env)
    print(" Spark Session Created")

    # 1. Load Config
    config = get_app_config(job_run_env)
    output_path = config["output.path"]  

    # 2. Read Raw Data
    print("\n Reading Input Data...")
    customers_df = read_customers(spark, job_run_env)
    products_df = read_products(spark, job_run_env)
    sales_df = read_sales(spark, job_run_env)
    print(" DataFrames Loaded")

    # 3. Clean & Transform
    print("\n Handling Missing Values...")
    customers_df, products_df, sales_df = handle_missing_values(customers_df, products_df, sales_df)

    print(" Removing Duplicates...")
    customers_df, products_df, sales_df = remove_duplicates(customers_df, products_df, sales_df)

    print(" Joining DataFrames...")
    joined_df = join_dataframes(sales_df, customers_df, products_df)

    print(" Adding Partition Columns...")
    final_df = add_partition_columns(joined_df)

    print(" Transformations Complete")
    print(f" Total Records After Join: {final_df.count()}")

    # 4. Write Parquet
    print("\n Writing Final DataFrame as Partitioned Parquet...")
    write_partitioned_parquet(final_df, output_path)
    print(f" Written Successfully to: {output_path}")

    # 5. Read Back Parquet
    print("\n Loading Final Data for Analytics...")
    df = load_final_dataframe(spark, output_path)
    print(" Analytical Data Loaded")

    # 6. Run Analytics
    print("\n====== ANALYTICS OUTPUT ======\n")

    print(" TOTAL SALES BY CATEGORY")
    total_sales_by_category(df).show()

    print(" HIGHEST PURCHASE CUSTOMER")
    highest_purchase_customer(df).show()

    print(" AVERAGE DISCOUNT")
    average_discount(df).show()

    print(" UNIQUE PRODUCTS BY REGION")
    unique_products_by_region(df).show()

    print(" TOTAL PROFIT BY STATE")
    total_profit_by_state(df).show()

    print(" HIGHEST SALES SUBCATEGORY")
    highest_sales_subcategory(df).show()

    print(" AVERAGE AGE BY SEGMENT")
    average_age_by_segment(df).show()

    print(" SHIPPED ORDERS BY MODE")
    shipped_orders_by_mode(df).show()

    print(" TOTAL QUANTITY BY CITY")
    total_quantity_by_city(df).show()

    print(" PROFIT MARGIN BY SEGMENT")
    profit_margin_by_segment(df).show()

    print("\n END OF MAIN — JOB COMPLETED SUCCESSFULLY \n")
