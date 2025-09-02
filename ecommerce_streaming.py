from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
from pyspark.sql.functions import expr
import argparse

def create_spark_session(app_name, checkpoint_location):
    """Create Spark session with optimized configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", checkpoint_location) \
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .getOrCreate()

def define_order_schema():
    """Define the schema for incoming order data"""
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("is_fraud_simulation", BooleanType(), True)
    ])

def read_kafka_stream(spark, kafka_servers, topic):
    """Read streaming data from Kafka"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("kafka.request.timeout.ms", "60000") \
        .option("kafka.session.timeout.ms", "30000") \
        .option("kafka.heartbeat.interval.ms", "10000") \
        .option("kafka.connections.max.idle.ms", "300000") \
        .option("kafka.metadata.max.age.ms", "300000") \
        .load()

def parse_and_clean_data(raw_stream, schema):
    """Parse JSON data and add necessary columns"""
    return raw_stream \
        .selectExpr("CAST(value AS STRING) as json_string", "timestamp as kafka_timestamp") \
        .select(from_json(col("json_string"), schema).alias("data"), col("kafka_timestamp")) \
        .select("data.*", col("kafka_timestamp")) \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .filter(col("event_timestamp").isNotNull()) \
        .withWatermark("event_timestamp", "30 seconds")

def create_windowed_aggregations(parsed_stream):
    """Create windowed aggregations for business metrics"""
    return parsed_stream \
        .groupBy(
            window(col("event_timestamp"), "1 minute", "30 seconds"),  # 1-min window, 30-sec slide
            col("category"),
            col("location")
        ) \
        .agg(
            count("*").alias("order_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            approx_count_distinct("user_id").alias("unique_customers"),
            max("total_amount").alias("max_order_value"),
            min("total_amount").alias("min_order_value")
        ) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .drop("window") \
        .filter(col("order_count") > 0)

def create_fraud_detection(parsed_stream):
    """Detect potential fraudulent transactions"""
    return parsed_stream \
        .filter(
            (col("total_amount") > 1000) |  # High value orders
            (col("location").isin(["XX", "YY", "ZZ"])) |  # Suspicious locations
            (col("is_fraud_simulation") == True)  # Simulated fraud
        ) \
        .select(
            col("order_id"),
            col("user_id"),
            col("product_name"),
            col("total_amount"),
            col("location"),
            col("event_timestamp"),
            when(col("total_amount") > 1000, "HIGH_VALUE_ORDER")
            .when(col("location").isin(["XX", "YY", "ZZ"]), "SUSPICIOUS_LOCATION")
            .when(col("is_fraud_simulation") == True, "FRAUD_SIMULATION")
            .otherwise("UNKNOWN").alias("alert_type"),
            current_timestamp().alias("alert_timestamp")
        )

def write_to_s3_parquet(df, output_path, checkpoint_path, trigger_interval="10 seconds"):
    """Write DataFrame to S3 in Parquet format"""
    return df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .trigger(processingTime=trigger_interval) \
        .start()

def write_to_kafka(df, kafka_servers, topic, checkpoint_path):
    """Write DataFrame back to Kafka as alerts"""
    return df \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_path) \
        .option("kafka.request.timeout.ms", "60000") \
        .option("kafka.session.timeout.ms", "30000") \
        .option("kafka.heartbeat.interval.ms", "10000") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

def write_to_console(df, trigger_interval="30 seconds"):
    """Write DataFrame to console for monitoring"""
    return df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime=trigger_interval) \
        .start()

def main():
    parser = argparse.ArgumentParser(description='E-commerce Real-time Analytics')
    parser.add_argument('--kafka-servers', required=True, help='Kafka bootstrap servers')
    parser.add_argument('--input-topic', default='ecommerce-orders', help='Input Kafka topic')
    parser.add_argument('--alert-topic', default='ecommerce-alerts', help='Alert Kafka topic')
    parser.add_argument('--s3-bucket', required=True, help='S3 bucket for output')
    parser.add_argument('--checkpoint-bucket', required=True, help='S3 bucket for checkpoints')
    
    args = parser.parse_args()
    
    print(f"Starting with args: {args}")
    
    # Create Spark session
    spark = create_spark_session(
        "ECommerceRealtimeAnalytics",
        f"s3://{args.checkpoint_bucket}/main-checkpoint/"
    )
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("Defining schema...")
        schema = define_order_schema()
        
        print("Creating sample stream...")
        # Use sample data instead of Kafka
        sample_stream = spark \
            .readStream \
            .format("rate") \
            .option("rowsPerSecond", 10) \
            .load() \
            .withColumn("order_id", expr("concat('order_', value)")) \
            .withColumn("user_id", expr("concat('user_', value % 100)")) \
            .withColumn("category", expr("case when value % 4 = 0 then 'Electronics' else 'Clothing' end")) \
            .withColumn("product_name", expr("case when value % 4 = 0 then 'MacBook Pro' else 'Nike Shoes' end")) \
            .withColumn("location", expr("case when value % 3 = 0 then 'US' else 'UK' end")) \
            .withColumn("total_amount", expr("(value % 500) + 50.0")) \
            .withColumn("is_fraud_simulation", expr("value % 10 = 0")) \
            .withColumn("event_timestamp", col("timestamp")) \
            .withWatermark("event_timestamp", "30 seconds")
        
        parsed_stream = sample_stream
        
        print("Creating aggregations...")
        windowed_agg = create_windowed_aggregations(parsed_stream)
        
        print("Setting up fraud detection...")
        fraud_alerts = create_fraud_detection(parsed_stream)
        
        print("Starting output streams...")
        
        # Output 1: Write aggregations to S3
        s3_writer = write_to_s3_parquet(
            windowed_agg,
            f"s3://{args.s3_bucket}/windowed-aggregations/",
            f"s3://{args.checkpoint_bucket}/s3-checkpoint/"
        )
        
        # Output 2: Write alerts to Kafka
        alert_writer = write_to_kafka(
            fraud_alerts,
            args.kafka_servers,
            args.alert_topic,
            f"s3://{args.checkpoint_bucket}/alert-checkpoint/"
        )
        
        # Output 3: Console monitoring
        console_writer = write_to_console(windowed_agg)
        
        print("Streaming job started successfully!")
        print("Press Ctrl+C to stop...")
        
        # Wait for termination
        console_writer.awaitTermination()
        
    except Exception as e:
        print(f"Error in streaming job: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        print("Stopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()