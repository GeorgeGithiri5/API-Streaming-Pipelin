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
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    grt = GoodreadsTransform(spark)
    
    # Modules in the project
    modules = {
        "author.csv": grt.transform_author_dataset,
        "book.csv": grt.transform_books_dataset,
        "reviews.csv": grt.transform_reviews_dataset,
        "user.csv": grt.transform_users_dataset
    }
    
    logging.debug("\n\nCopying data from s3 landing zone to ...")
    gds3 = GoodReadsS3Module()
    gds3.s3_move_data(source_bucket = config.get('BUCKET', 'LANDING_ZONE'), target_bucket = config.get('BUCKET', 'WORKING_ZONE'))
    
    files_in_working_zone = gds3.get_files(config.get('BUCKET', 'WORKING_ZONE'))
    
    # Cleanup proceed zone if files available in working zone
    
    if len([set(modules.keys()) & set(files_in_working_zone)]) > 0:
        logging.info("Cleaning up processed zone.")
        gds3.clean_bucket(config.get('BUCKET', 'PROCESSED_ZONE'))
        
    for file in files_in_working_zone:
        if file in modules.keys():
            modules[file]()
            
    logging.debug("Waiting before setting up warehouse")
    time.sleep(5)
    
    