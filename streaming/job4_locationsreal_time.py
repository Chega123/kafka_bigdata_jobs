from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import sys

try:
    from elk_sender import create_elk_sender
    ELK_AVAILABLE = True
except ImportError:
    print("elk_sender.py no encontrado - continuando sin ELK")
    ELK_AVAILABLE = False


class LocationTrackerSystem:
    def __init__(self, 
                 kafka_servers='b-1.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092,b-2.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092',
                 enable_elk=False,
                 elk_host='localhost'):
        
        self.spark = SparkSession.builder \
            .appName("CryptoLocationTracker") \
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.kafka_servers = kafka_servers

        self.s3_base = "s3://bitcoin-bigdata/processed"
        self.s3_locations = f"{self.s3_base}/locations"
        
        self.checkpoint_dir = "s3://bitcoin-bigdata/checkpoints/job8"
        
        self.BLACKLIST_LOCATIONS = [
            "WORLDWIDE", "EVERYWHERE", "GLOBAL", "NOWHERE", "INTERNET", 
            "MOON", "MARS", "EARTH", "SPACE", "ONLINE", "VIRTUAL",
            "CRYPTO", "BITCOIN", "ETHEREUM", "BLOCKCHAIN", "WEB3",
            "METAVERSE", "NFT", "DEFI", "WORLD", "UNIVERSE"
        ]
        
        self.elk = None
        if ELK_AVAILABLE and enable_elk:
            self.elk = create_elk_sender(
                enabled=True,
                es_host=elk_host,
                use_aws_auth=True,
                region='us-east-1'
            )
            
            if self.elk and self.elk.enabled:
                location_mapping = {
                    "mappings": {
                        "properties": {
                            "crypto_type": {"type": "keyword"},
                            "location_clean": {"type": "keyword"},
                            "original_location": {"type": "text"},
                            "user_name": {"type": "keyword"},
                            "user_followers": {"type": "integer"},
                            "user_verified": {"type": "boolean"},
                            "tweet_text": {"type": "text"},
                            "timestamp": {"type": "date"},
                            "job_name": {"type": "keyword"}
                        }
                    }
                }
                
                metrics_mapping = {
                    "mappings": {
                        "properties": {
                            "window_start": {"type": "date"},
                            "window_end": {"type": "date"},
                            "crypto_type": {"type": "keyword"},
                            "location_clean": {"type": "keyword"},
                            "total_tweets": {"type": "integer"},
                            "unique_users": {"type": "integer"},
                            "avg_followers": {"type": "float"},
                            "timestamp": {"type": "date"}
                        }
                    }
                }
                
                self.elk.create_index("crypto-locations-job8", location_mapping)
                self.elk.create_index("crypto-location-metrics-job8", metrics_mapping)
        
        print(f"\nINICIANDO RASTREADOR DE UBICACIONES")
        print(f"Kafka: {kafka_servers}")
        print(f"Topics: bitcoin-tweets, ethereum-tweets")
        print(f"S3 Output: {self.s3_base}")
        print(f"ELK: {'Habilitado' if self.elk and self.elk.enabled else 'Deshabilitado'}")
        if self.elk and self.elk.enabled:
            print(f"ELK Host: {elk_host}")
        print(f"Blacklist locations: {len(self.BLACKLIST_LOCATIONS)}")

    def define_schema(self):
        return StructType([
            StructField("crypto_type", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("user_location", StringType(), True),
            StructField("user_description", StringType(), True),
            StructField("user_created", StringType(), True),
            StructField("user_followers", IntegerType(), True),
            StructField("user_friends", IntegerType(), True),
            StructField("user_favourites", IntegerType(), True),
            StructField("user_verified", BooleanType(), True),
            StructField("date", StringType(), True),
            StructField("text", StringType(), True),
            StructField("hashtags", StringType(), True),
            StructField("source", StringType(), True),
            StructField("is_retweet", BooleanType(), True),
            StructField("timestamp", StringType(), True)
        ])
    
    def read_kafka_stream(self):
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "bitcoin-tweets,ethereum-tweets") \
            .option("startingOffsets", "latest") \
            .load()
        
        schema = self.define_schema()
        tweets_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        
        return tweets_df

    def process_locations(self, tweets_df):
        """Limpia y estandariza ubicaciones"""
        
        clean_df = tweets_df.filter(col("user_location").isNotNull()) \
                            .filter(col("user_location") != "") \
                            .filter(col("user_location") != "Unknown")
        
        clean_df = clean_df.withColumn(
            "location_clean",
            upper(trim(regexp_replace(col("user_location"), "[📍🌍🌎🌏🗺️]", "")))
        )
        
        clean_df = clean_df.filter(length(col("location_clean")) > 2) \
                           .filter(~col("location_clean").isin(self.BLACKLIST_LOCATIONS))
        
        clean_df = clean_df.filter(~col("location_clean").contains("HTTP")) \
                           .filter(~col("location_clean").contains("WWW.")) \
                           .filter(~col("location_clean").startswith("#"))
        
        result_df = clean_df.select(
            "crypto_type",
            "location_clean",
            col("user_location").alias("original_location"),
            "user_name",
            "user_followers",
            "user_verified",
            col("text").alias("tweet_text"),
            "timestamp"
        )
        
        return result_df

    def print_console(self, batch_df, batch_id):
        row_count = batch_df.count()  
        if row_count == 0:
            return
        
        print(f"\nLOCATION TRACKER - Batch #{batch_id} - {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 80)
        print(f"Ubicaciones válidas detectadas: {row_count}")
 
        print("\nTop 10 ubicaciones:")
        top_locations = batch_df.groupBy("crypto_type", "location_clean") \
            .count() \
            .orderBy(desc("count")) \
            .limit(10)
        top_locations.show(truncate=False)
        
        print("\nResumen por cryptocurrency:")
        summary = batch_df.groupBy("crypto_type").agg(
            count("*").alias("total_tweets"),
            approx_count_distinct("location_clean").alias("unique_locations"),  
            approx_count_distinct("user_name").alias("unique_users")  
        )
        summary.show(truncate=False)

    def send_to_elk(self, batch_df, batch_id):
        """Envía datos a Elasticsearch"""
        if self.elk and self.elk.enabled:
            batch_with_job = batch_df.withColumn("job_name", lit("job8_location_tracker"))
            self.elk.send_batch_from_spark(batch_with_job, batch_id, "crypto-locations-job8")

    def calculate_metrics(self, locations_df):
        """Calcula metricas agregadas por ubicacion"""
        metrics_df = locations_df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window("timestamp", "5 minutes", "1 minute"),
                "crypto_type",
                "location_clean"
            ).agg(
                count("*").alias("total_tweets"),
                approx_count_distinct("user_name").alias("unique_users"),  
                avg("user_followers").alias("avg_followers")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "crypto_type",
                "location_clean",
                "total_tweets",
                "unique_users",
                "avg_followers"
            ).withColumn("timestamp", current_timestamp())
        
        return metrics_df

    def print_metrics(self, batch_df, batch_id):
        """Imprime métricas agregadas"""
        if batch_df.count() == 0:
            return
        
        print(f"\nLOCATION METRICS - Batch #{batch_id} - {datetime.now().strftime('%H:%M:%S')}")
        batch_df.orderBy(desc("total_tweets")).show(20, truncate=False)

    def send_metrics_to_elk(self, batch_df, batch_id):
        """Envía métricas a Elasticsearch"""
        if self.elk and self.elk.enabled:
            self.elk.send_batch_from_spark(batch_df, batch_id, "crypto-location-metrics-job8")

    def start(self):
        try:
            tweets_df = self.read_kafka_stream()
            
            print("\nIniciando rastreo de ubicaciones...")
            
            location_df = self.process_locations(tweets_df)
            
            query_s3 = location_df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_locations) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/locations_s3") \
                .trigger(processingTime='60 seconds') \
                .start()
            
            query_console = location_df.writeStream \
                .outputMode("append") \
                .foreachBatch(self.print_console) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/console") \
                .trigger(processingTime='60 seconds') \
                .start()
            
            if self.elk and self.elk.enabled:
                query_elk = location_df.writeStream \
                    .outputMode("append") \
                    .foreachBatch(self.send_to_elk) \
                    .option("checkpointLocation", f"{self.checkpoint_dir}/elk") \
                    .trigger(processingTime='60 seconds') \
                    .start()
            
            metrics_df = self.calculate_metrics(location_df)
            
            query_metrics_s3 = metrics_df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", f"{self.s3_base}/location_metrics") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/metrics_s3") \
                .trigger(processingTime='60 seconds') \
                .start()
            
            query_metrics_console = metrics_df.writeStream \
                .outputMode("append") \
                .foreachBatch(self.print_metrics) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/metrics_console") \
                .trigger(processingTime='60 seconds') \
                .start()
            
            if self.elk and self.elk.enabled:
                query_metrics_elk = metrics_df.writeStream \
                    .outputMode("append") \
                    .foreachBatch(self.send_metrics_to_elk) \
                    .option("checkpointLocation", f"{self.checkpoint_dir}/metrics_elk") \
                    .trigger(processingTime='60 seconds') \
                    .start()
            
            print(f"\nS3 Locations: {self.s3_locations}")
            print(f"\nEsperando datos de Kafka...\n")
            
            query_console.awaitTermination()
            
        except KeyboardInterrupt:
            print("\nSistema detenido correctamente")
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Location Tracker System')
    parser.add_argument('--kafka',
                       default='b-1.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092,b-2.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092',
                       help='Servidores Kafka')
    parser.add_argument('--elk-host',
                       default='localhost',
                       help='Host de Elasticsearch')
    parser.add_argument('--enable-elk',
                       action='store_true',
                       help='Habilitar envío a Elasticsearch')
    
    args = parser.parse_args()
    
    tracker = LocationTrackerSystem(
        kafka_servers=args.kafka,
        enable_elk=args.enable_elk,
        elk_host=args.elk_host
    )
    
    tracker.start()


if __name__ == "__main__":
    main()