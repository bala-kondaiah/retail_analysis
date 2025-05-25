import pytest
from lib.utility import  get_spark_session
from lib.dataReader import read_customers,read_orders
from lib.dataManipulation import filter_closed_orders,filter_orders_generic
from lib.configReader import get_app_config

@pytest.mark.skip("work in progress")
def test_read_customers(spark):
    customers_count=read_customers(spark,"LOCAL").count()
    assert customers_count==12435

@pytest.mark.transformation()
def test_read_orders(spark):
    orders_count=read_orders(spark,"LOCAL").count()
    assert orders_count==68884
    
@pytest.mark.slow()
def test_filter_closed_order(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count=filter_closed_orders(orders_df).count() 
    assert filtered_count==7556

def test_app_config():
    config=get_app_config("LOCAL")
    assert config["orders.file.path"]=="data/orders.csv"


@pytest.mark.skip()
def test_check_closed_count(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count=filter_orders_generic(orders_df,"CLOSED").count() 
    assert filtered_count==7556

@pytest.mark.skip()
def test_check_closed_count(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count=filter_orders_generic(orders_df,"COMPLETE").count() 
    assert filtered_count==7556

# so above two latest  lables code are repeating right , so we can avoid by using below code.
# thats why skipping 


@pytest.mark.parametrize(
         "status,count",
        [("CLOSED",7556),
         ("PENDING_PAYMENT",15030),
         ("COMPLETE",22900)
         ]
)

def test_check_count(spark,status,count):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count=filter_orders_generic(orders_df,status).count() 
    assert filtered_count==count

