from lib import ConfigReader

#defining customers schema
def get_customers_schema():
    schema = """
        customer_id string,
        customer_name string,
        segment string,
        age int,
        country string,
        city string,
        state string,
        postal_code int,
        region string
    """
    return schema

# creating customers dataframe
def read_customers(spark,env):
    conf = ConfigReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_customers_schema()) \
        .load(customers_file_path)

#defining products schema
def get_products_schema():
    schema = """
        product_id string,
        category string,
        sub_category string,
        product_name string
    """
    return schema

#creating products dataframe
def read_products(spark,env):
    conf = ConfigReader.get_app_config(env)
    products_file_path = conf["products.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_products_schema()) \
        .load(products_file_path)


#defining sales schema

def get_sales_schema():
    schema = """
        order_line int,
        order_id string,
        order_date date,
        ship_date date,
        ship_mode string,
        customer_id string,
        product_id string,
        sales double,
        quantity int,
        discount double,
        profit double    
    """
    return schema

#creating sales dataframe
def read_sales(spark,env):
    conf = ConfigReader.get_app_config(env)
    sales_file_path = conf["sales.file.path"]
    return spark.read \
        .format("csv")\
        .option("header", "true")\
        .schema(get_sales_schema())\
        .load(sales_file_path)
