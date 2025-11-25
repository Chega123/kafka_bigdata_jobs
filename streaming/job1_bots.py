# streaming/job6_bot_detection_aws.py
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


class BotDetectionSystem:
    def __init__(self, 
                 kafka_servers='b-1.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092,b-2.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092',
                 enable_elk=False,
                 elk_host='localhost'):
        
        self.spark = SparkSession.builder \
            .appName("CryptoBotDetection") \
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.kafka_servers = kafka_servers

        # Rutas S3
        self.s3_base = "s3://bitcoin-bigdata/processed"
        self.s3_bot_alerts = f"{self.s3_base}/bot_alerts"
        self.s3_bot_metrics = f"{self.s3_base}/bot_metrics"
        
        # Checkpoints en S3
        self.checkpoint_dir = "s3://bitcoin-bigdata/checkpoints/job6"

        # Umbrales y parámetros heurísticos
        self.HF_WINDOW_SECONDS = 60
        self.HF_MIN_TWEETS = 3
        self.FRIENDS_THRESHOLD = 2000
        self.FOLLOWERS_LOW_THRESHOLD = 100
        self.NEW_ACCOUNT_DAYS = 30
        self.DESCRIPTION_MIN_LENGTH = 5

        # Configurar ELK
        self.elk = None
        if ELK_AVAILABLE and enable_elk:
            self.elk = create_elk_sender(
                enabled=True,
                es_host=elk_host,
                use_aws_auth=True,
                region='us-east-1'
            )
            
            if self.elk and self.elk.enabled:
                bot_alerts_mapping = {
                    "mappings": {
                        "properties": {
                            "crypto_type": {"type": "keyword"},
                            "alert_type": {"type": "keyword"},
                            "alert_priority": {"type": "keyword"},
                            "alert_reason": {"type": "text"},
                            "user_name": {"type": "keyword"},
                            "user_followers": {"type": "integer"},
                            "user_friends": {"type": "integer"},
                            "user_verified": {"type": "boolean"},
                            "text": {"type": "text"},
                            "timestamp": {"type": "date"},
                            "job_name": {"type": "keyword"}
                        }
                    }
                }

                bot_metrics_mapping = {
                    "mappings": {
                        "properties": {
                            "window_start": {"type": "date"},
                            "window_end": {"type": "date"},
                            "crypto_type": {"type": "keyword"},
                            "alert_type": {"type": "keyword"},
                            "alert_priority": {"type": "keyword"},
                            "total_alerts": {"type": "integer"},
                            "unique_users": {"type": "integer"},
                            "metric_type": {"type": "keyword"},
                            "timestamp": {"type": "date"}
                        }
                    }
                }

                self.elk.create_index("crypto-bot-alerts-job6", bot_alerts_mapping)
                self.elk.create_index("crypto-bot-metrics-job6", bot_metrics_mapping)

        print(f"\nKafka: {kafka_servers}")
        print(f"Topics: bitcoin-tweets, ethereum-tweets")
        print(f"S3 Output: {self.s3_base}")
        print(f"ELK: {'Habilitado' if self.elk and self.elk.enabled else 'Deshabilitado'}")
        if self.elk and self.elk.enabled:
            print(f"ELK Host: {elk_host}")
        print(f"\nParámetros de detección:")
        print(f"  HIGH FREQUENCY: >= {self.HF_MIN_TWEETS} tweets en {self.HF_WINDOW_SECONDS}s")
        print(f"  FRIENDS_THRESHOLD: {self.FRIENDS_THRESHOLD}")
        print(f"  FOLLOWERS_LOW: {self.FOLLOWERS_LOW_THRESHOLD}")
        print(f"  NEW_ACCOUNT: < {self.NEW_ACCOUNT_DAYS} días")

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
         .withColumn("timestamp", to_timestamp(col("timestamp"))) \
         .withColumn("user_created_dt", to_timestamp(col("user_created")))

        return tweets_df

    def detect_high_frequency_posting(self, tweets_df):
        """Detecta usuarios que tweetean muy frecuentemente"""
        agg = tweets_df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window("timestamp", f"{self.HF_WINDOW_SECONDS} seconds"),
                "user_name",
                "crypto_type"
            ).agg(
                count("*").alias("tweet_count"),
                first("user_followers").alias("user_followers"),
                first("user_friends").alias("user_friends"),
                first("user_verified").alias("user_verified"),
                first("text").alias("text")
            ).filter(col("tweet_count") >= self.HF_MIN_TWEETS) \
            .withColumn("alert_type", lit("HIGH_FREQUENCY_POSTING")) \
            .withColumn("alert_priority", lit("HIGH")) \
            .withColumn("alert_reason", concat(
                lit("Usuario posteando mucho: "),
                col("user_name"),
                lit(" -> "),
                col("tweet_count").cast("string"),
                lit(" tweets en "),
                lit(str(self.HF_WINDOW_SECONDS)),
                lit("s")
            )) \
            .withColumn("timestamp", col("window").getField("end")) \
            .select(
                "crypto_type",
                "alert_type",
                "alert_priority",
                "alert_reason",
                "user_name",
                "user_followers",
                "user_friends",
                "user_verified",
                "text",
                "timestamp"
            )

        return agg

    def detect_unbalanced_friends_followers(self, tweets_df):
        """Detecta usuarios con muchos friends pero pocos followers"""
        ratio_df = tweets_df.filter(
            (col("user_friends") >= self.FRIENDS_THRESHOLD) &
            (col("user_followers") <= self.FOLLOWERS_LOW_THRESHOLD)
        ).withColumn("alert_type", lit("FRIENDS_FOLLOWERS_IMBALANCE")) \
         .withColumn("alert_priority", lit("MEDIUM")) \
         .withColumn("alert_reason", concat(
             lit("Relación extraña friends/followers para "),
             col("user_name"),
             lit(" (friends: "),
             col("user_friends").cast("string"),
             lit(", followers: "),
             col("user_followers").cast("string"),
             lit(")")
         )).select(
             "crypto_type",
             "alert_type",
             "alert_priority",
             "alert_reason",
             "user_name",
             "user_followers",
             "user_friends",
             "user_verified",
             "text",
             "timestamp"
         )

        return ratio_df

    def detect_empty_description(self, tweets_df):
        """Detecta perfiles con descripción vacía o muy corta"""
        desc_df = tweets_df.filter(
            (col("user_description").isNull()) |
            (length(trim(col("user_description"))) < self.DESCRIPTION_MIN_LENGTH)
        ).withColumn("alert_type", lit("EMPTY_PROFILE")) \
         .withColumn("alert_priority", lit("LOW")) \
         .withColumn("alert_reason", concat(
             lit("Perfil vacío/corto para "),
             col("user_name"),
             lit(" (desc length: "),
             length(coalesce(col("user_description"), lit(""))).cast("string"),
             lit(")")
         )).select(
             "crypto_type",
             "alert_type",
             "alert_priority",
             "alert_reason",
             "user_name",
             "user_followers",
             "user_friends",
             "user_verified",
             "text",
             "timestamp"
         )

        return desc_df

    def detect_new_accounts(self, tweets_df):
        """Detecta cuentas creadas hace menos de NEW_ACCOUNT_DAYS días"""
        new_acc_df = tweets_df.filter(col("user_created_dt").isNotNull()) \
            .withColumn("account_age_days", datediff(current_date(), to_date(col("user_created_dt")))) \
            .filter(col("account_age_days") < self.NEW_ACCOUNT_DAYS) \
            .withColumn("alert_type", lit("NEW_ACCOUNT_SUSPICIOUS")) \
            .withColumn("alert_priority", lit("MEDIUM")) \
            .withColumn("alert_reason", concat(
                lit("Cuenta nueva ("),
                col("account_age_days").cast("string"),
                lit(" días) - "),
                col("user_name")
            )).select(
                "crypto_type",
                "alert_type",
                "alert_priority",
                "alert_reason",
                "user_name",
                "user_followers",
                "user_friends",
                "user_verified",
                "text",
                "timestamp"
            )

        return new_acc_df

    def print_alerts(self, batch_df, batch_id):
        """Impresión formateada de alertas"""
        if batch_df.count() == 0:
            return

        print(f"\n🤖 BOT ALERTS - Batch #{batch_id} - {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 80)
        
        crypto_types = [row['crypto_type'] for row in batch_df.select("crypto_type").distinct().collect()]

        for crypto in sorted(crypto_types):
            print(f"\n{crypto.upper()}")
            print("-" * 80)
            crypto_alerts = batch_df.filter(col("crypto_type") == crypto).collect()

            for alert in crypto_alerts:
                priority_icon = {
                    "CRITICAL": "PELIGROSO",
                    "HIGH": "ALTO",
                    "MEDIUM": "CUIDADO",
                    "LOW": "NAH"
                }.get(alert['alert_priority'], "?")

                print(f"\n{priority_icon} [{alert['alert_type']}] - Priority: {alert['alert_priority']}")
                print(f"   {alert['alert_reason']}")
                if alert['text']:
                    print(f"   Tweet: {alert['text'][:100]}...")
                print(f"   Time: {alert['timestamp']}")

        # Resumen
        print("\n📊 RESUMEN:")
        summary = batch_df.groupBy("crypto_type", "alert_type", "alert_priority").count()
        summary.orderBy("crypto_type", "alert_type").show(truncate=False)

    def send_alerts_to_elk(self, batch_df, batch_id):
        if self.elk and self.elk.enabled:
            batch_with_job = batch_df.withColumn("job_name", lit("job6_bot_detection"))
            self.elk.send_batch_from_spark(batch_with_job, batch_id, "crypto-bot-alerts-job6")

    def calculate_metrics(self, alerts_df):
        """Calcula métricas sobre las alertas"""
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
            ).withColumn("metric_type", lit("job6_bot_detection")) \
             .withColumn("timestamp", current_timestamp())

        return metrics_df

    def print_metrics(self, batch_df, batch_id):
        if batch_df.count() == 0:
            return
        print(f"\n📈 METRICS - Batch #{batch_id} - {datetime.now().strftime('%H:%M:%S')}")
        batch_df.orderBy("crypto_type", "alert_type").show(truncate=False)

    def send_metrics_to_elk(self, batch_df, batch_id):
        if self.elk and self.elk.enabled:
            self.elk.send_batch_from_spark(batch_df, batch_id, "crypto-bot-metrics-job6")

    def start(self):
        try:
            tweets_df = self.read_kafka_stream()
            
            print("\nIniciando detección de bots...")

            # Generar alertas por cada heurística
            alerts_hf = self.detect_high_frequency_posting(tweets_df)
            alerts_ratio = self.detect_unbalanced_friends_followers(tweets_df)
            alerts_desc = self.detect_empty_description(tweets_df)
            alerts_new = self.detect_new_accounts(tweets_df)

            # Unir todas las alertas
            all_alerts = alerts_hf.unionByName(alerts_ratio, allowMissingColumns=True) \
                                   .unionByName(alerts_desc, allowMissingColumns=True) \
                                   .unionByName(alerts_new, allowMissingColumns=True)

            # Guardar alertas en S3
            query_s3_alerts = all_alerts.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_bot_alerts) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/alerts_s3") \
                .trigger(processingTime='60 seconds') \
                .start()

            # Mostrar en consola
            query_console = all_alerts.writeStream \
                .outputMode("append") \
                .foreachBatch(self.print_alerts) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/alerts_console") \
                .trigger(processingTime='60 seconds') \
                .start()

            # Enviar a ELK
            if self.elk and self.elk.enabled:
                query_elk = all_alerts.writeStream \
                    .outputMode("append") \
                    .foreachBatch(self.send_alerts_to_elk) \
                    .option("checkpointLocation", f"{self.checkpoint_dir}/alerts_elk") \
                    .trigger(processingTime='60 seconds') \
                    .start()

            # Métricas
            metrics_df = self.calculate_metrics(all_alerts)
            
            query_s3_metrics = metrics_df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_bot_metrics) \
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

            print(f"\nS3 Alerts: {self.s3_bot_alerts}")
            print(f"S3 Metrics: {self.s3_bot_metrics}")
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

    parser = argparse.ArgumentParser(description='Bot Detection System')
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

    bot_system = BotDetectionSystem(
        kafka_servers=args.kafka,
        enable_elk=args.enable_elk,
        elk_host=args.elk_host
    )

    bot_system.start()


if __name__ == "__main__":
    main()