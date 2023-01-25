from pyspark.sql import SparkSession
from transform import GoodreadsTransform
from s3_module import GoodReadsS3Module
from pathlib import Path
import logging
import logging.config
import configparser
import time
from warehouse.goodreads_warehouse_driver import GoodReadsWarehouseDriver

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

def create_sparksession():
    return SparkSession.builder.master('yarn').appName("goodreads") \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
        .enableHiveSupport().getOrCreate()
        
def main():
    