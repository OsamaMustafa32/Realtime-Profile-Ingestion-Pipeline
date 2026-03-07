"""
Spark Streaming Pipeline: Kafka → Cassandra

This script consumes user records from a Kafka topic ('users_data'),
transforms them using PySpark Structured Streaming, and writes the results
to Apache Cassandra. It uses the Spark Cassandra Connector for sink writes.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster
import logging


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
    """)
    print("Cassandra -> Keyspace created successfully!")
    print("**************************************************")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Cassandra Table created successfully!")
    print("********************************************************")

def create_spark_connection():
    """
    Create and return a SparkSession configured for Kafka source and Cassandra sink.
    Uses Spark 2.13-compatible Kafka and Cassandra connector packages.
    """
    s_conn = None

    try:
        print("Creating Spark session!")
        print("**************************************************")
        # Kafka connector and Cassandra connector JARs; Cassandra host for writes
        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark session created successfully!")
        print("**************************************************")
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        print("**************************************************")
    
    return s_conn

def connect_to_kafka(spark_conn):
    """
    Create a streaming DataFrame from the Kafka topic 'users_data'.
    Uses 'latest' so only new messages are read (no backfill).
    """
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "users_data") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("kafka dataframe created successfully!")
        print("**************************************************")
    except Exception as e:
        print(f"kafka dataframe could not be created because: {e}")
        print("********************************************************")

    return spark_df

def create_cassandra_connection():
    """Connect to the local Cassandra cluster and return a session for DDL (keyspace/table)."""
    try:
        cluster = Cluster(['localhost'])

        cass_session = cluster.connect()
        print("Cassandra session created successfully!")
        print("**************************************************")
        return cass_session
    except Exception as e:
        print(f"Cassandra session could not be created because: {e}")
        print("********************************************************")
        return None

def create_selection_df_from_kafka(spark_df):
    """
    Parse Kafka 'value' (JSON string) into a structured DataFrame.
    Schema must match the JSON produced by the Kafka producer.
    """
    # Schema for user records from the Kafka topic
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Cast binary value to string, parse JSON, then flatten to columns
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

if __name__ == "__main__":
    # 1. Create Spark session with Kafka + Cassandra support
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        # 2. Read stream from Kafka topic 'users_data'
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        # 3. Connect to Cassandra and create keyspace/table
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            print("Streaming is being started...")
            print("********************************************************")

            # 4. Write streaming DataFrame to Cassandra; checkpoint for fault tolerance
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'created_users')
                                .start()
                                )

            streaming_query.awaitTermination()


