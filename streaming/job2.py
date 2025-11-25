from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime
import sys

try:
    from elk_sender import create_elk_sender
    ELK_AVAILABLE = True
except ImportError:
    print("elk_sender.py no encontrado - continuando sin ELK")
    ELK_AVAILABLE = False


class TrendingTopicsDetector:
    def __init__(self, 
                 kafka_servers='b-1.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092,b-2.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092',
                 enable_elk=False,
                 elk_host='localhost'):
        
        self.spark = SparkSession.builder \
            .appName("CryptoTrendingTopics") \
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.kafka_servers = kafka_servers
        
        # Configurar ELK si está disponible
        self.elk = None
        if ELK_AVAILABLE and enable_elk:
            self.elk = create_elk_sender(
                enabled=True, 
                es_host=elk_host,
                use_aws_auth=True,
                region='us-east-1'  # Tu región
            )
                        
            if self.elk and self.elk.enabled:
                hashtags_mapping = {
                    "mappings": {
                        "properties": {
                            "crypto_type": {"type": "keyword"},
                            "hashtag": {"type": "keyword"},
                            "count": {"type": "integer"},
                            "window_start": {"type": "date"},
                            "window_end": {"type": "date"},
                            "timestamp": {"type": "date"},
                            "job_name": {"type": "keyword"}
                        }
                    }
                }
                
                trending_mapping = {
                    "mappings": {
                        "properties": {
                            "rank": {"type": "integer"},
                            "crypto_type": {"type": "keyword"}, 
                            "hashtag": {"type": "keyword"},
                            "count": {"type": "integer"},
                            "percentage": {"type": "float"},
                            "window_start": {"type": "date"},
                            "window_end": {"type": "date"},
                            "timestamp": {"type": "date"},
                            "job_name": {"type": "keyword"}
                        }
                    }
                }
                
                metrics_mapping = {
                    "mappings": {
                        "properties": {
                            "crypto_type": {"type": "keyword"},
                            "total_tweets": {"type": "integer"},
                            "total_hashtags": {"type": "integer"},
                            "unique_hashtags": {"type": "integer"},
                            "avg_hashtags_per_tweet": {"type": "float"},
                            "window_start": {"type": "date"},
                            "window_end": {"type": "date"},
                            "timestamp": {"type": "date"},
                            "job_name": {"type": "keyword"}
                        }
                    }
                }
                
                self.elk.create_index("crypto-hashtags-job2", hashtags_mapping)
                self.elk.create_index("crypto-trending-job2", trending_mapping)
                self.elk.create_index("crypto-hashtag-metrics-job2", metrics_mapping)
        
        # Rutas S3
        self.s3_base = "s3://bitcoin-bigdata/processed"
        self.s3_hashtags = f"{self.s3_base}/hashtags"
        self.s3_trending = f"{self.s3_base}/trending"
        self.s3_metrics = f"{self.s3_base}/hashtag_metrics"
        
        # Checkpoints en S3
        self.checkpoint_dir = "s3://bitcoin-bigdata/checkpoints/job2"
        
        print(f"\nKafka: {kafka_servers}")
        print(f"Topics: bitcoin-tweets, ethereum-tweets")
        print(f"S3 Output: {self.s3_base}")
        print(f"ELK: {'Habilitado' if self.elk and self.elk.enabled else 'Deshabilitado'}")
        if self.elk and self.elk.enabled:
            print(f"ELK Host: {elk_host}")

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
    
    def extract_hashtags(self, tweets_df):
        hashtags_df = tweets_df \
            .filter(col("hashtags").isNotNull()) \
            .filter(col("hashtags") != "[]") \
            .filter(col("hashtags") != "") \
            .withColumn(
                "hashtags_clean",
                regexp_replace(col("hashtags"), "[\\[\\]']", "")
            ) \
            .withColumn(
                "hashtags_array",
                split(col("hashtags_clean"), ", ")
            ) \
            .select(
                "crypto_type",
                "user_name",
                "user_verified",
                "is_retweet",
                "timestamp",
                explode("hashtags_array").alias("hashtag")
            ) \
            .filter(col("hashtag") != "") \
            .withColumn(
                "hashtag",
                lower(trim(col("hashtag")))
            )
        
        return hashtags_df
    
    def calculate_trending_hashtags(self, hashtags_df):
        trending_df = hashtags_df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window("timestamp", "5 minutes", "1 minute"),
                "crypto_type", 
                "hashtag"
            ).agg(
                count("*").alias("count")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "crypto_type",
                "hashtag",
                "count"
            ) \
            .withColumn(
                "timestamp",
                current_timestamp()
            ) \
            .withColumn(
                "job_name",
                lit("job2_trending")
            )
        
        return trending_df
    
    def get_top_trending(self, batch_df, batch_id):
        if batch_df.count() == 0:
            return
        
        print(f"\nTRENDING TOPICS - Batch #{batch_id} - {datetime.now().strftime('%H:%M:%S')}")
        windows_cryptos = batch_df.select("window_start", "window_end", "crypto_type").distinct().collect()

        windows = {}
        for row in windows_cryptos:
            key = (row['window_start'], row['window_end'])
            if key not in windows:
                windows[key] = []
            windows[key].append(row['crypto_type'])
        
        for (window_start, window_end), crypto_types in windows.items():
            print(f"\nVentana: {window_start.strftime('%H:%M:%S')} - {window_end.strftime('%H:%M:%S')}")
            print("=" * 80)

            for crypto_type in sorted(set(crypto_types)):
                print(f"\n{crypto_type.upper()}")
                print("-" * 80)
                
                window_data = batch_df.filter(
                    (col("window_start") == window_start) &
                    (col("window_end") == window_end) &
                    (col("crypto_type") == crypto_type)
                ).orderBy(col("count").desc()).limit(10)
                
                top_hashtags = window_data.collect()
                
                if len(top_hashtags) > 0:
                    import builtins
                    total = builtins.sum([h['count'] for h in top_hashtags])
                    
                    print(f"{'Rank':<6} {'Hashtag':<25} {'Count':<10} {'%':<10}")
                    print("-" * 80)
                    
                    for idx, row in enumerate(top_hashtags, 1):
                        hashtag = row['hashtag']
                        count = row['count']
                        percentage = (count / total * 100) if total > 0 else 0
                        
                        print(f"{idx:<6} #{hashtag:<24} {count:<10} {percentage:>5.1f}%")
                    
                    if self.elk and self.elk.enabled:
                        self.send_top_to_elk(top_hashtags, window_start, window_end, total, crypto_type)
    
    def send_top_to_elk(self, top_hashtags, window_start, window_end, total, crypto_type):
        for idx, row in enumerate(top_hashtags, 1):
            doc = {
                'rank': idx,
                'crypto_type': crypto_type,  
                'hashtag': row['hashtag'],
                'count': row['count'],
                'percentage': (row['count'] / total * 100) if total > 0 else 0,
                'window_start': window_start.isoformat(),
                'window_end': window_end.isoformat(),
                'timestamp': datetime.now().isoformat(),
                'job_name': 'job2_trending'
            }
            self.elk.send_document("crypto-trending-job2", doc)
    
    def send_hashtags_to_elk(self, batch_df, batch_id):
        if self.elk and self.elk.enabled:
            self.elk.send_batch_from_spark(batch_df, batch_id, "crypto-hashtags-job2")
    
    def calculate_metrics(self, tweets_df):
        metrics_df = tweets_df \
            .filter(col("hashtags").isNotNull()) \
            .filter(col("hashtags") != "[]") \
            .withWatermark("timestamp", "2 minutes") \
            .withColumn(
                "hashtags_clean",
                regexp_replace(col("hashtags"), "[\\[\\]']", "")
            ) \
            .withColumn(
                "hashtags_array",
                split(col("hashtags_clean"), ", ")
            ) \
            .withColumn(
                "num_hashtags",
                size("hashtags_array")
            ) \
            .groupBy(
                window("timestamp", "5 minutes", "1 minute"),
                "crypto_type"
            ).agg(
                count("*").alias("total_tweets"),
                sum("num_hashtags").alias("total_hashtags"),
                approx_count_distinct("hashtags").alias("unique_hashtags"),
                avg("num_hashtags").alias("avg_hashtags_per_tweet")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "crypto_type",
                "total_tweets",
                "total_hashtags",
                "unique_hashtags",
                "avg_hashtags_per_tweet"
            ).withColumn(
                "timestamp",
                current_timestamp()
            ).withColumn(
                "job_name",
                lit("job2_metrics")
            )
        
        return metrics_df
    
    def print_metrics(self, batch_df, batch_id):
        if batch_df.count() > 0:
            print(f"\nMETRICS - Batch #{batch_id}")
            batch_df.show(truncate=False)
    
    def send_metrics_to_elk(self, batch_df, batch_id):
        if self.elk and self.elk.enabled:
            self.elk.send_batch_from_spark(batch_df, batch_id, "crypto-hashtag-metrics-job2")
    
    def start(self):
        try:
            tweets_df = self.read_kafka_stream()
            hashtags_df = self.extract_hashtags(tweets_df)
            
            print("\nIniciando queries de streaming...")
            
            query_s3_raw = hashtags_df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_hashtags) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/hashtags_raw") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            trending_df = self.calculate_trending_hashtags(hashtags_df)
            
            query_s3_trending = trending_df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_trending) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/trending_s3") \
                .trigger(processingTime='60 seconds') \
                .start()
            
            query_console = trending_df.writeStream \
                .outputMode("append") \
                .foreachBatch(self.get_top_trending) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/trending_console") \
                .trigger(processingTime='60 seconds') \
                .start()
            
            if self.elk and self.elk.enabled:
                query_elk = trending_df.writeStream \
                    .outputMode("append") \
                    .foreachBatch(self.send_hashtags_to_elk) \
                    .option("checkpointLocation", f"{self.checkpoint_dir}/trending_elk") \
                    .trigger(processingTime='60 seconds') \
                    .start()
            
            metrics_df = self.calculate_metrics(tweets_df)
            
            query_s3_metrics = metrics_df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_metrics) \
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
            
            print(f"\nS3 Hashtags: {self.s3_hashtags}")
            print(f"S3 Trending: {self.s3_trending}")
            print(f"S3 Metrics: {self.s3_metrics}")
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
    
    parser = argparse.ArgumentParser(description='Crypto Trending Topics Detector')
    parser.add_argument('--kafka', 
                       default='b-1.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092,b-2.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092',
                       help='Servidores Kafka')
    parser.add_argument('--elk-host',
                       default='localhost',
                       help='Host de Elasticsearch')
    parser.add_argument('--enable-elk',
                       action='store_true',
                       help='Habilitar envio a Elasticsearch')
    
    args = parser.parse_args()
    
    detector = TrendingTopicsDetector(
        kafka_servers=args.kafka,
        enable_elk=args.enable_elk,
        elk_host=args.elk_host
    )
    
    detector.start()


if __name__ == "__main__":
    main()