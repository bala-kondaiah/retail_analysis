import configparser
from pyspark import SparkConf

# loading the application configs in python dictionary
def get_app_config(env):
    config=configparser.ConfigParser()
    config.read("configs/application.conf")
    app_level_conf={}
    for(key,value) in config.items(env):
        app_level_conf[key]=value
    return app_level_conf

# loading the pyspark configs and creating a spark conf object

def get_pyspark_conf(env):
    config=configparser.ConfigParser()
    config.read("configs/pyspark.conf")
    sprak_level_config=SparkConf()
    for(key,value) in config.items(env):
        sprak_level_config.set(key,value)
    return sprak_level_config

# we want to read the spark as object because whenever spark session , so we can pass the same object to the session.




