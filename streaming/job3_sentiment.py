# streaming/job7_sentiment_aws.py
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


class SentimentSystem:
    def __init__(self, 
                 kafka_servers='b-1.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092,b-2.bigdatakafla.x86hxo.c24.kafka.us-east-1.amazonaws.com:9092',
                 enable_elk=False,
                 elk_host='localhost'):
        
        self.spark = SparkSession.builder \
            .appName("CryptoSentimentAnalysis") \
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.kafka_servers = kafka_servers

        # Rutas S3
        self.s3_base = "s3://bitcoin-bigdata/processed"
        self.s3_sentiment_alerts = f"{self.s3_base}/sentiment_alerts"
        self.s3_sentiment_metrics = f"{self.s3_base}/sentiment_metrics"
        
        # Checkpoints en S3
        self.checkpoint_dir = "s3://bitcoin-bigdata/checkpoints/job7"

        # Parámetros heurísticos
        self.STRONG_POS_EMOJI_COUNT = 2
        self.STRONG_NEG_EXCLAMATION = 3
        self.UPPERCASE_RATIO_THRESHOLD = 0.40
        self.MIN_TEXT_LENGTH_FOR_UPPERCASE = 10
        
        self.POSITIVE_KEYWORDS = [
            'good', 'great', 'awesome', 'amazing', 'bullish', 'moon', 'up',
            'gains', 'profit', 'pumped', 'strong', 'love', 'happy', 'win', 'green'
        ]
        self.NEGATIVE_KEYWORDS = [
            'bad', 'terrible', 'angry', 'hate', 'drop', 'crash', 'bearish', 'dip',
            'loss', 'rekt', 'fear', 'panic', 'scam', 'fraud', 'dump', 'red'
        ]
        
        # Emojis (versión simplificada para evitar problemas de encoding)
        self.POSITIVE_EMOJIS = ['😀', '😁', '😂', '🤣', '😊', '😍', '🤩', '😉', '👍', '🙌', '🎉', '💰', '🚀', '🌕', '✨']
        self.NEGATIVE_EMOJIS = ['😡', '😠', '😤', '😞', '😢', '😭', '💩', '👎', '😱', '😨', '😓', '😒']

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
                alerts_mapping = {
                    "mappings": {
                        "properties": {
                            "crypto_type": {"type": "keyword"},
                            "sentiment_type": {"type": "keyword"},
                            "alert_priority": {"type": "keyword"},
                            "alert_reason": {"type": "text"},
                            "user_name": {"type": "keyword"},
                            "user_followers": {"type": "integer"},
                            "user_verified": {"type": "boolean"},
                            "text": {"type": "text"},
                            "sentiment_score": {"type": "float"},
                            "pos_emoji_count": {"type": "integer"},
                            "neg_emoji_count": {"type": "integer"},
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
                            "sentiment_type": {"type": "keyword"},
                            "alert_priority": {"type": "keyword"},
                            "total_alerts": {"type": "integer"},
                            "unique_users": {"type": "integer"},
                            "avg_score": {"type": "float"},
                            "timestamp": {"type": "date"}
                        }
                    }
                }
                
                self.elk.create_index("crypto-sentiment-alerts-job7", alerts_mapping)
                self.elk.create_index("crypto-sentiment-metrics-job7", metrics_mapping)

        print(f"\nKafka: {kafka_servers}")
        print(f"Topics: bitcoin-tweets, ethereum-tweets")
        print(f"S3 Output: {self.s3_base}")
        print(f"ELK: {'Habilitado' if self.elk and self.elk.enabled else 'Deshabilitado'}")
        if self.elk and self.elk.enabled:
            print(f"ELK Host: {elk_host}")
        print(f"\nParámetros de análisis de sentimiento:")
        print(f"  Positive keywords: {len(self.POSITIVE_KEYWORDS)}")
        print(f"  Negative keywords: {len(self.NEGATIVE_KEYWORDS)}")

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

    def _register_sentiment_udf(self):
        """UDF que analiza sentimiento usando heurísticas"""
        
        POS_EMOJI = set(self.POSITIVE_EMOJIS)
        NEG_EMOJI = set(self.NEGATIVE_EMOJIS)
        pos_keys = self.POSITIVE_KEYWORDS
        neg_keys = self.NEGATIVE_KEYWORDS
        upper_ratio_thr = self.UPPERCASE_RATIO_THRESHOLD
        min_len_upper = self.MIN_TEXT_LENGTH_FOR_UPPERCASE
        strong_pos_emoji = self.STRONG_POS_EMOJI_COUNT
        strong_neg_excl = self.STRONG_NEG_EXCLAMATION

        def analyze(text):
            if text is None or text == "":
                return ("NEUTRAL_SENTIMENT", "LOW", "Empty text", 0.0, 0, 0)
            
            txt = text.strip()
            txt_lower = txt.lower()

            # Contar emojis
            pos_emoji_count = sum(1 for emoji in POS_EMOJI if emoji in txt)
            neg_emoji_count = sum(1 for emoji in NEG_EMOJI if emoji in txt)

            # Ratio de mayúsculas
            letters = [c for c in txt if c.isalpha()]
            total_letters = len(letters)
            uppercase_letters = sum(1 for c in letters if c.isupper())
            upper_ratio = (uppercase_letters / total_letters) if total_letters > 0 else 0.0

            # Puntuación
            exclamations = txt.count("!")
            repeated_excl = exclamations >= strong_neg_excl

            # Keywords
            pos_hits = sum(1 for k in pos_keys if k in txt_lower)
            neg_hits = sum(1 for k in neg_keys if k in txt_lower)

            # Score calculation
            score = 0.0
            score += pos_emoji_count * 2
            score -= neg_emoji_count * 2
            score += pos_hits * 1
            score -= neg_hits * 1
            
            if repeated_excl:
                score -= 3
            if upper_ratio > upper_ratio_thr and len(txt) >= min_len_upper:
                score -= 2

            # Determinar sentimiento
            sentiment_type = "NEUTRAL_SENTIMENT"
            alert_priority = "LOW"
            alert_reason = "No strong signals detected"

            # Strong positive
            if pos_emoji_count >= strong_pos_emoji or pos_hits >= 2 or score >= 3:
                sentiment_type = "STRONG_POSITIVE_SENTIMENT"
                alert_priority = "HIGH"
                alert_reason = f"Strong positive: emojis={pos_emoji_count}, keywords={pos_hits}, score={score:.1f}"
            
            # Positive
            elif pos_emoji_count >= 1 or pos_hits >= 1 or score >= 2:
                sentiment_type = "POSITIVE_SENTIMENT"
                alert_priority = "MEDIUM"
                alert_reason = f"Positive: emojis={pos_emoji_count}, keywords={pos_hits}"
            
            # Strong negative
            elif neg_emoji_count >= 1 and (exclamations >= strong_neg_excl or neg_hits >= 2):
                sentiment_type = "STRONG_NEGATIVE_SENTIMENT"
                alert_priority = "CRITICAL"
                alert_reason = f"Strong negative: emojis={neg_emoji_count}, excl={exclamations}, score={score:.1f}"
            
            # Negative
            elif neg_hits >= 1 or exclamations >= 2 or (upper_ratio > upper_ratio_thr and len(txt) >= min_len_upper):
                sentiment_type = "NEGATIVE_SENTIMENT"
                alert_priority = "HIGH"
                alert_reason = f"Negative: keywords={neg_hits}, excl={exclamations}, caps={upper_ratio:.2f}"

            return (sentiment_type, alert_priority, alert_reason, float(score), int(pos_emoji_count), int(neg_emoji_count))

        out_schema = StructType([
            StructField("sentiment_type", StringType(), True),
            StructField("alert_priority", StringType(), True),
            StructField("alert_reason", StringType(), True),
            StructField("score", FloatType(), True),
            StructField("pos_emoji_count", IntegerType(), True),
            StructField("neg_emoji_count", IntegerType(), True),
        ])

        return udf(analyze, out_schema)

    def detect_sentiment(self, tweets_df):
        """Aplica análisis de sentimiento a los tweets"""
        sentiment_udf = self._register_sentiment_udf()
        
        analyzed = tweets_df.withColumn("sent", sentiment_udf(col("text")))

        alerts = analyzed.withColumn("sentiment_type", col("sent.sentiment_type")) \
                         .withColumn("alert_priority", col("sent.alert_priority")) \
                         .withColumn("alert_reason", col("sent.alert_reason")) \
                         .withColumn("sentiment_score", col("sent.score")) \
                         .withColumn("pos_emoji_count", col("sent.pos_emoji_count")) \
                         .withColumn("neg_emoji_count", col("sent.neg_emoji_count")) \
                         .select(
                             "crypto_type",
                             "sentiment_type",
                             "alert_priority",
                             "alert_reason",
                             "user_name",
                             "user_followers",
                             "user_verified",
                             "text",
                             "timestamp",
                             "sentiment_score",
                             "pos_emoji_count",
                             "neg_emoji_count"
                         )

        return alerts

    def print_alerts(self, batch_df, batch_id):
        """Impresión formateada de alertas de sentimiento"""
        if batch_df.count() == 0:
            return

        print(f"\nSENTIMENT ALERTS - Batch #{batch_id} - {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 80)
        
        crypto_types = [row['crypto_type'] for row in batch_df.select("crypto_type").distinct().collect()]

        for crypto in sorted(crypto_types):
            print(f"\n{crypto.upper()}")
            print("-" * 80)
            crypto_alerts = batch_df.filter(col("crypto_type") == crypto).collect()

            for alert in crypto_alerts:
                sentiment = alert['sentiment_type']
                
                priority_marker = {
                    "CRITICAL": "[!!!]",
                    "HIGH": "[!!]",
                    "MEDIUM": "[!]",
                    "LOW": "[i]"
                }.get(alert['alert_priority'], "[?]")

                print(f"\n{priority_marker} [{sentiment}] - Priority: {alert['alert_priority']}")
                print(f"   {alert['alert_reason']}")
                if alert['text']:
                    print(f"   Tweet: {alert['text'][:120]}...")
                print(f"   Score: {alert['sentiment_score']:.1f} | +emojis: {alert['pos_emoji_count']} | -emojis: {alert['neg_emoji_count']}")
                print(f"   Time: {alert['timestamp']}")

        # Resumen
        print("\nRESUMEN:")
        summary = batch_df.groupBy("crypto_type", "sentiment_type", "alert_priority").count()
        summary.orderBy("crypto_type", "sentiment_type").show(truncate=False)

    def send_alerts_to_elk(self, batch_df, batch_id):
        if self.elk and self.elk.enabled:
            batch_with_job = batch_df.withColumn("job_name", lit("job7_sentiment"))
            self.elk.send_batch_from_spark(batch_with_job, batch_id, "crypto-sentiment-alerts-job7")

    def calculate_metrics(self, alerts_df):
        """Calcula métricas sobre el sentimiento"""
        metrics_df = alerts_df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window("timestamp", "5 minutes", "1 minute"),
                "crypto_type",
                "sentiment_type",
                "alert_priority"
            ).agg(
                count("*").alias("total_alerts"),
                approx_count_distinct("user_name").alias("unique_users"),
                avg("sentiment_score").alias("avg_score")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "crypto_type",
                "sentiment_type",
                "alert_priority",
                "total_alerts",
                "unique_users",
                "avg_score"
            ).withColumn("timestamp", current_timestamp())

        return metrics_df

    def print_metrics(self, batch_df, batch_id):
        if batch_df.count() == 0:
            return
        print(f"\nSENTIMENT METRICS - Batch #{batch_id} - {datetime.now().strftime('%H:%M:%S')}")
        batch_df.orderBy("crypto_type", "sentiment_type").show(truncate=False)

    def send_metrics_to_elk(self, batch_df, batch_id):
        if self.elk and self.elk.enabled:
            self.elk.send_batch_from_spark(batch_df, batch_id, "crypto-sentiment-metrics-job7")

    def start(self):
        try:
            tweets_df = self.read_kafka_stream()
            
            print("\nIniciando análisis de sentimiento...")

            # Detectar sentimiento
            alerts = self.detect_sentiment(tweets_df)

            # Guardar en S3
            query_s3_alerts = alerts.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_sentiment_alerts) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/alerts_s3") \
                .trigger(processingTime='60 seconds') \
                .start()

            # Mostrar en consola
            query_console = alerts.writeStream \
                .outputMode("append") \
                .foreachBatch(self.print_alerts) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/alerts_console") \
                .trigger(processingTime='60 seconds') \
                .start()

            # Enviar a ELK
            if self.elk and self.elk.enabled:
                query_elk = alerts.writeStream \
                    .outputMode("append") \
                    .foreachBatch(self.send_alerts_to_elk) \
                    .option("checkpointLocation", f"{self.checkpoint_dir}/alerts_elk") \
                    .trigger(processingTime='60 seconds') \
                    .start()

            # Métricas
            metrics_df = self.calculate_metrics(alerts)
            
            query_s3_metrics = metrics_df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", self.s3_sentiment_metrics) \
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

            print(f"\nS3 Alerts: {self.s3_sentiment_alerts}")
            print(f"S3 Metrics: {self.s3_sentiment_metrics}")
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

    parser = argparse.ArgumentParser(description='Sentiment Analysis System')
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

    sentiment_system = SentimentSystem(
        kafka_servers=args.kafka,
        enable_elk=args.enable_elk,
        elk_host=args.elk_host
    )

    sentiment_system.start()


if __name__ == "__main__":
    main()