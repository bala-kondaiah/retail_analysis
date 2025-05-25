from lib import configReader

# define the customer schema

def get_customer_schema(): 
    schema="customer_id int, customer_fname string, customer_lname string, username string"
    return schema

# creating customer dataFrame 

def read_customers(spark,env):
    conf=configReader.get_app_config(env)
    customer_file_path=conf["customer.file.path"]
    return spark.read \
        .format("csv") \
        .option("header","true") \
        .schema(get_customer_schema()) \
        .load(customer_file_path)


# define the orders schema

def get_orders_schema(): 
    schema="order_id int, order_date string, customer_id int, order_status string"
    return schema           


# creating order dataFrame 

def read_orders(spark,env):
    conf=configReader.get_app_config(env)
    orders_file_path=conf["orders.file.path"]
    return spark.read \
        .format("csv") \
        .option("header","true") \
        .schema(get_orders_schema()) \
        .load(orders_file_path)



