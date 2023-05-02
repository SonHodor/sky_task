from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging

class SparkConnectPg():
    
    def __init__(self, host, port, db, user, password):
        self.conn = None
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        
        self.spark = (
            SparkSession
                .builder
                .config("spark.jars", "path to driver/postgresql-42.3.1.jar")
                .master("local")
                .appName("increment")
                .getOrCreate())
    
    
    def pg_read_df(self, db_table):
        return (self.spark.read
                .format("jdbc")
                .option("url", f"jdbc:postgresql://{self.host}:{self.port}/{self.db}")
                .option("driver", "org.postgresql.Driver")
                .option("dbtable", db_table)
                .option("user", self.user)
                .option("password", self.password)
                .load())
    
    
    def pg_write(self, df: DataFrame, db_table, save_mode = 'append'):
        (df
            .write.format("jdbc")
            .mode(save_mode)
            .option("url", f"jdbc:postgresql://{self.host}:{self.port}/{self.db}")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", db_table)
            .option("user", self.user)
            .option("password", self.password)
            .save())