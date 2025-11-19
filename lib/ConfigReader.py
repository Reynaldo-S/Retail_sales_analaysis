import configparser
from pyspark import SparkConf

#loading file path for the application conf
def get_app_config(env):
    config = configparser.ConfigParser()
    config.read('configs/application.conf')
    app_conf={}

    for (key,val) in config.items(env):
        app_conf[key]=val
    return app_conf

print(get_app_config('LOCAL'))

# loading the pyspark configs
def get_pyspark_config(env):
    config = configparser.ConfigParser()
    config.read("configs/pyspark.conf")
    pyspark_conf = SparkConf()
    for (key,val) in config.items(env):
        pyspark_conf.set(key,val)
    return pyspark_conf

print(get_pyspark_config('LOCAL'))