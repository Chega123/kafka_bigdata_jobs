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


class AlertSystem:
    def __init__(self, 
                 kafka_servers='b-1.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092,b-2.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092',
                 enable_elk=False,
                 elk_host='localhost'):
        
        self.spark = SparkSession.builder \
            .appName("CryptoAlertsSystem") \
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
                region='us-east-1'
            )
            
            if self.elk and self.elk.enabled:
                alerts_mapping = {
                    "mappings": {
                        "properties": {
                            "crypto_type": {"type": "keyword"},
                            "alert_type": {"type": "keyword"},
                            "alert_priority": {"type": "keyword"},
                            "alert_reason": {"type": "text"},
                            "user_name": {"type": "keyword"},
                            "user_followers": {"type": "integer"},
                            "user_verified": {"type": "boolean"},
                            "text": {"type": "text"},
                            "timestamp": {"type": "date"},
                            "job_name": {"type": "keyword"}
                        }
                    }
                }
                
                metrics_mapping = {
                    "mappings": {
                        "properties": {
                            "crypto_type": {"type": "keyword"},
                            "window_start": {"type": "date"},
                            "window_end": {"type": "date"},
                            "alert_type": {"type": "keyword"},
                            "alert_priority": {"type": "keyword"},
                            "total_alerts": {"type": "integer"},
                            "unique_users": {"type": "integer"},
                            "metric_type": {"type": "keyword"},
                            "timestamp": {"type": "date"}
                        }
                    }
                }
                
                self.elk.create_index("crypto-alerts-job5", alerts_mapping)
                self.elk.create_index("crypto-metrics-job5", metrics_mapping)
        
        # Rutas S3
        self.s3_base = "s3://bitcoin-bigdata/processed"
        self.s3_raw = f"{self.s3_base}/raw_tweets"
        self.s3_alerts = f"{self.s3_base}/alerts"
        
        # Checkpoints en S3
        self.checkpoint_dir = "s3://bitcoin-bigdata/checkpoints/job5"
        
        # Umbrales
        self.VIRAL_THRESHOLD = 100
        self.HIGH_REACH_THRESHOLD = 10000
        
        print(f"\nKafka: {kafka_servers}")
        print(f"Topics: bitcoin-tweets, ethereum-tweets")
        print(f"S3 Output: {self.s3_base}")
        print(f"ELK: {'Habilitado' if self.elk and self.elk.enabled else 'Deshabilitado'}")
        if self.elk and self.elk.enabled:
            print(f"ELK Host: {elk_host}")
            print(f"Indices:")
            print(f"  - crypto-alerts-job5")
            print(f"  - crypto-metrics-job5")
        print(f"VIRAL_THRESHOLD: {self.VIRAL_THRESHOLD} retweets")
        print(f"HIGH_REACH_THRESHOLD: {self.HIGH_REACH_THRESHOLD} seguidores")
    
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
    
    def detect_viral_tweets(self, tweets_df):
        viral_df = tweets_df.filter(
            (col("is_retweet") == True) | 
            (col("text").contains("RT @"))
        ).withColumn(
            "alert_type", lit("VIRAL_TWEET")
        ).withColumn(
            "alert_priority", lit("HIGH")
        ).withColumn(
            "alert_reason", 
            concat(
                lit("["),
                upper(col("crypto_type")),
                lit("] Tweet con potencial viral - Usuario: "),
                col("user_name"),
                lit(" | Seguidores: "),
                col("user_followers").cast("string")
            )
        ).select(
            "crypto_type",  
            "alert_type",
            "alert_priority",
            "alert_reason",
            "user_name",
            "user_followers",
            "user_verified",
            "text",
            "timestamp"
        )
        
        return viral_df
    
    def detect_high_reach_users(self, tweets_df):
        vip_df = tweets_df.filter(
            col("user_followers") >= self.HIGH_REACH_THRESHOLD
        ).withColumn(
            "alert_type", lit("HIGH_REACH_USER")
        ).withColumn(
            "alert_priority", 
            when(col("user_verified") == True, "CRITICAL")
            .when(col("user_followers") >= 50000, "HIGH")
            .otherwise("MEDIUM")
        ).withColumn(
            "alert_reason",
            concat(
                lit("["),
                upper(col("crypto_type")),
                lit("] Usuario VIP activo - "),
                col("user_name"),
                lit(" ("),
                col("user_followers").cast("string"),
                lit(" seguidores) - Verificado: "),
                col("user_verified").cast("string")
            )
        ).select(
            "crypto_type", 
            "alert_type",
            "alert_priority",
            "alert_reason",
            "user_name",
            "user_followers",
            "user_verified",
            "text",
            "timestamp"
        )
        
        return vip_df
    
    def print_alerts(self, batch_df, batch_id):
        if batch_df.count() == 0:
            return

        print(f"ALERTAS - BATCH #{batch_id} - {datetime.now().strftime('%H:%M:%S')}")
        crypto_types = [row['crypto_type'] for row in batch_df.select("crypto_type").distinct().collect()]
        
        for crypto in sorted(crypto_types):
            print(f"\n{crypto.upper()}")
            
            crypto_alerts = batch_df.filter(col("crypto_type") == crypto).collect()
            
            for alert in crypto_alerts:
                priority_icon = {
                    "CRITICAL": "[CRITICAL]",
                    "HIGH": "[HIGH]", 
                    "MEDIUM": "[MEDIUM]",
                    "LOW": "[LOW]"
                }.get(alert['alert_priority'], "NAH")
                
                print(f"\n{priority_icon} [{alert['alert_type']}] - Prioridad: {alert['alert_priority']}")
                print(f"   {alert['alert_reason']}")
                print(f"   Tweet: {alert['text'][:100]}...")
                print(f"   Timestamp: {alert['timestamp']}")
        
        print("\nResumen del batch:")
        summary = batch_df.groupBy("crypto_type", "alert_type", "alert_priority").count()
        summary.orderBy("crypto_type", "alert_type").show(truncate=False)
    
    def send_alerts_to_elk(self, batch_df, batch_id):
        if self.elk and self.elk.enabled:
            batch_with_job = batch_df.withColumn("job_name", lit("job5_alerts"))
            self.elk.send_batch_from_spark(batch_with_job, batch_id, "crypto-alerts-job5")
    
    def calculate_metrics(self, alerts_df):
        metrics_df = alerts_df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window("timestamp", "5 minutes", "1 minute"),
                "crypto_type",
                "alert_type",
                "alert_priority"
            ).agg(
                count("*").alias("total_alerts"),
                approx_count_distinct("user_name").alias("unique_users")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "crypto_type",
                "alert_type",
                "alert_priority",
                "total_alerts",
                "unique_users"
            ).withColumn(
                "metric_type", lit("job5_alerts")
            ).withColumn(
                "timestamp", current_timestamp()
            )
        
        return metrics_df
    
    def print_metrics(self, batch_df, batch_id):
        if batch_df.count() == 0:
            return
        
        print(f"\n{'='*80}")
        print(f"MÉTRICAS DE ALERTAS - Batch #{batch_id}")
        print(f"{'='*80}")
        batch_df.orderBy("crypto_type", "alert_type", "alert_priority").show(truncate=False)
        
        print("\nResumen por Crypto:")
        summary = batch_df.groupBy("crypto_type").agg(
            sum("total_alerts").alias("total_alerts"),
            sum("unique_users").alias("total_unique_users")
        )
        summary.orderBy("crypto_type").show(truncate=False)
    
    def send_metrics_to_elk(self, batch_df, batch_id):
        if self.elk and self.elk.enabled:
            self.elk.send_batch_from_spark(batch_df, batch_id, "crypto-metrics-job5")
    
    def start_monitoring(self):
        try:
            tweets_df = self.read_kafka_stream()
            
            print("\nIniciando queries de streaming...")
            
            query_raw = tweets_df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_raw) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/raw") \
                .trigger(processingTime='30 seconds') \
                .start()            

            viral_alerts = self.detect_viral_tweets(tweets_df)
            vip_alerts = self.detect_high_reach_users(tweets_df)
            all_alerts = viral_alerts.union(vip_alerts)
            
            query_alerts_s3 = all_alerts.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_alerts) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/alerts_s3") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            
            query_alerts_console = all_alerts.writeStream \
                .outputMode("append") \
                .foreachBatch(self.print_alerts) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/alerts_console") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            if self.elk and self.elk.enabled:
                query_alerts_elk = all_alerts.writeStream \
                    .outputMode("append") \
                    .foreachBatch(self.send_alerts_to_elk) \
                    .option("checkpointLocation", f"{self.checkpoint_dir}/alerts_elk") \
                    .trigger(processingTime='10 seconds') \
                    .start()
            

            metrics_df = self.calculate_metrics(all_alerts)
            
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
            
            print(f"\nS3 Raw Tweets: {self.s3_raw}")
            print(f"S3 Alerts: {self.s3_alerts}")
            print(f"\nEsperando datos de Kafka...\n")
            
            query_alerts_console.awaitTermination()
            
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
    
    parser = argparse.ArgumentParser(description='Sistema de Alertas en Tiempo Real')
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
    
    alert_system = AlertSystem(
        kafka_servers=args.kafka,
        enable_elk=args.enable_elk,
        elk_host=args.elk_host
    )
    
    alert_system.start_monitoring()


if __name__ == "__main__":
    main()