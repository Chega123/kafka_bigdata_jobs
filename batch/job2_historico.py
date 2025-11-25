
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import sys

class BatchHistoricalAnalyzer:
    def __init__(self):
        print("JOB 2: ANALISIS HISTORICO BATCH")
        
        self.spark = SparkSession.builder \
            .appName("Job2_BatchHistoricalAnalysis") \
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.s3_base = "s3://bitcoin-bigdata/analytics"
        self.s3_output = f"{self.s3_base}/batch_results/historical_analysis"
        
        self.known_bots = [
            "IFTTT", "dlvr.it", "vaiotapi", "TweetDeck", "ContentStudio", 
            "Microsoft Power Platform", "abnormal_crypto_app", "exchangewhales",
            "Hootsuite", "Buffer", "ClankApp", "Zapier", "TwinyBots", 
            "Bot", "bot", "Robot", "API", "Feed"
        ]
    
    def read_bitcoin_csv(self, csv_path):
        print(f"Leyendo Bitcoin CSV: {csv_path}")
        
        col_names = [
            'user_name', 'user_location', 'user_description', 'user_created',
            'user_followers', 'user_friends', 'user_favourites', 'user_verified',
            'date', 'text', 'hashtags', 'source', 'is_retweet'
        ]
        
        df = self.spark.read.csv(csv_path, header=False, inferSchema=True)
        
        for i, name in enumerate(col_names):
            df = df.withColumnRenamed(f"_c{i}", name)
        
        df = df.withColumn("crypto_type", lit("bitcoin"))
        df = df.fillna({
            'user_followers': 0,
            'user_verified': False,
            'source': 'Unknown'
        })
        
        count = df.count()
        print(f"Bitcoin: {count:,} registros cargados")
        
        return df
    
    def read_ethereum_csv(self, csv_path):
        print(f"Leyendo Ethereum CSV: {csv_path}")
        
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        
        df = df.withColumn("crypto_type", lit("ethereum"))
        df = df.fillna({
            'user_followers': 0,
            'user_verified': False,
            'source': 'Unknown'
        })
        
        count = df.count()
        print(f"Ethereum: {count:,} registros cargados\n")
        
        return df
    
    def combine_and_prepare(self, btc_df, eth_df):
        print("Combinando datasets...")
        
        combined_df = btc_df.union(eth_df)
        
        prepared_df = combined_df.select(
            col("crypto_type"),
            col("user_name"),
            col("user_followers").cast("integer").alias("followers"),
            col("user_verified"),
            col("source"),
            to_date(col("date")).alias("tweet_date"),
            col("text"),
            col("is_retweet")
        )
        
        prepared_df.cache()
        
        total = prepared_df.count()
        print(f"Total combinado: {total:,} registros\n")
        
        return prepared_df
    
    def analyze_daily_volume(self, df):
        print("ANALISIS 1: VOLUMEN DIARIO")
        
        daily_volume = df.groupBy("tweet_date", "crypto_type") \
            .agg(
                count("*").alias("total_tweets"),
                countDistinct("user_name").alias("unique_users")
            ) \
            .orderBy(desc("total_tweets"))
        
        print("\nTop 20 dias mas activos:")
        daily_volume.show(20, truncate=False)
        
        print("\nResumen por cryptocurrency:")
        summary = daily_volume.groupBy("crypto_type") \
            .agg(
                sum("total_tweets").alias("total_tweets"),
                avg("total_tweets").alias("avg_daily_tweets"),
                max("total_tweets").alias("max_daily_tweets")
            )
        summary.show(truncate=False)
        
        return daily_volume
    
    def analyze_top_influencers(self, df):
        print("ANALISIS 2: TOP INFLUENCERS")
        
        influencers = df.groupBy("user_name", "crypto_type") \
            .agg(
                max("followers").alias("max_followers"),
                count("text").alias("total_tweets"),
                first("user_verified").alias("is_verified")
            ) \
            .orderBy(desc("max_followers"))
        
        print("\nTop 30 influencers globales:")
        influencers.show(30, truncate=False)
        
        print("\nTop 15 Bitcoin:")
        influencers.filter(col("crypto_type") == "bitcoin").show(15, truncate=False)
        
        print("\nTop 15 Ethereum:")
        influencers.filter(col("crypto_type") == "ethereum").show(15, truncate=False)
        
        return influencers
    
    def analyze_devices_and_bots(self, df):
        print("ANALISIS 3: PLATAFORMAS Y BOTS")
        
        bot_condition = lower(col("source")).rlike("|".join([b.lower() for b in self.known_bots]))
        
        devices_df = df.withColumn("device_category",
            when(bot_condition, "Bots y Automatizacion")
            .when(col("source").contains("Web App"), "PC Web Browser")
            .when(col("source").contains("Android"), "Android")
            .when(
                (col("source").contains("iPhone")) | 
                (col("source").contains("iPad")) | 
                (col("source").contains("Mac")), 
                "Apple iOS Mac"
            )
            .otherwise("Otras Apps")
        )
        
        device_stats = devices_df.groupBy("crypto_type", "device_category") \
            .agg(
                count("*").alias("total_tweets"),
                countDistinct("user_name").alias("unique_users")
            ) \
            .orderBy("crypto_type", desc("total_tweets"))
        
        print("\nDistribucion por plataforma:")
        device_stats.show(50, truncate=False)
        
        print("\nAnalisis de automatizacion:")
        bot_percentage = devices_df.groupBy("crypto_type") \
            .agg(
                count("*").alias("total"),
                sum(when(col("device_category").contains("Bot"), 1).otherwise(0)).alias("bot_tweets")
            ) \
            .withColumn("bot_percentage", round((col("bot_tweets") / col("total")) * 100, 2))
        
        bot_percentage.show(truncate=False)
        
        return device_stats
    
    def analyze_credibility(self, df):
        print("ANALISIS 4: CREDIBILIDAD CUENTAS VERIFICADAS")
        
        verified_stats = df.groupBy("crypto_type", "user_verified") \
            .agg(
                count("*").alias("total_tweets"),
                countDistinct("user_name").alias("unique_users")
            )
        
        print("\nDistribucion de verificacion:")
        verified_stats.show(truncate=False)
        
        print("\nPorcentajes por crypto:")
        verification_pct = df.groupBy("crypto_type") \
            .agg(
                count("*").alias("total_tweets"),
                sum(when(col("user_verified") == True, 1).otherwise(0)).alias("verified_tweets")
            ) \
            .withColumn("verified_percentage", 
                       round((col("verified_tweets") / col("total_tweets")) * 100, 2))
        
        verification_pct.show(truncate=False)
        
        print("\nTop 20 usuarios verificados mas activos:")
        top_verified = df.filter(col("user_verified") == True) \
            .groupBy("user_name", "crypto_type") \
            .agg(
                count("*").alias("total_tweets"),
                max("followers").alias("followers")
            ) \
            .orderBy(desc("total_tweets"))
        
        top_verified.show(20, truncate=False)
        
        return verified_stats
    
    def generate_summary(self, df):
        print("RESUMEN EJECUTIVO")
        
        summary = df.groupBy("crypto_type") \
            .agg(
                count("*").alias("total_tweets"),
                countDistinct("user_name").alias("unique_users"),
                countDistinct("tweet_date").alias("days_with_activity"),
                sum(when(col("user_verified") == True, 1).otherwise(0)).alias("verified_tweets"),
                sum(when(col("is_retweet") == True, 1).otherwise(0)).alias("retweets"),
                avg("followers").alias("avg_followers")
            )
        
        print("\n")
        summary.show(truncate=False)
        
        return summary
    
    def save_results(self, daily_volume, influencers, device_stats, verified_stats, summary):
        print("GUARDANDO RESULTADOS EN S3")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            print("\nGuardando volumen diario...")
            daily_volume.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/daily_volume_{timestamp}.csv")
            print(f"Guardado: {self.s3_output}/daily_volume_{timestamp}.csv")
            
            print("\nGuardando top influencers...")
            influencers.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/top_influencers_{timestamp}.csv")
            print(f"Guardado: {self.s3_output}/top_influencers_{timestamp}.csv")
            
            print("\nGuardando analisis de dispositivos...")
            device_stats.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/device_stats_{timestamp}.csv")
            print(f"Guardado: {self.s3_output}/device_stats_{timestamp}.csv")
            
            print("\nGuardando estadisticas de verificacion...")
            verified_stats.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/verified_stats_{timestamp}.csv")
            print(f"Guardado: {self.s3_output}/verified_stats_{timestamp}.csv")
            
            print("\nGuardando resumen ejecutivo...")
            summary.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/executive_summary_{timestamp}.csv")
            print(f"Guardado: {self.s3_output}/executive_summary_{timestamp}.csv")
            
            print("\nGuardando versiones Parquet...")
            daily_volume.write.mode("overwrite").parquet(f"{self.s3_output}/parquet/daily_volume_{timestamp}")
            influencers.write.mode("overwrite").parquet(f"{self.s3_output}/parquet/influencers_{timestamp}")
            device_stats.write.mode("overwrite").parquet(f"{self.s3_output}/parquet/devices_{timestamp}")
            
            print("\nTodos los resultados guardados exitosamente")
            
        except Exception as e:
            print(f"\nError al guardar resultados: {e}")
            import traceback
            traceback.print_exc()
    
    def run(self, btc_csv, eth_csv):
        try:
            btc_df = self.read_bitcoin_csv(btc_csv)
            eth_df = self.read_ethereum_csv(eth_csv)
            
            combined_df = self.combine_and_prepare(btc_df, eth_df)
            
            daily_volume = self.analyze_daily_volume(combined_df)
            influencers = self.analyze_top_influencers(combined_df)
            device_stats = self.analyze_devices_and_bots(combined_df)
            verified_stats = self.analyze_credibility(combined_df)
            summary = self.generate_summary(combined_df)
            
            self.save_results(daily_volume, influencers, device_stats, verified_stats, summary)
            
            print("\n" + "="*80)
            print("ANALISIS COMPLETADO EXITOSAMENTE")
            print("="*80 + "\n")
            
        except Exception as e:
            print(f"\nError en el analisis: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Job 2: Analisis Historico Batch (Bitcoin + Ethereum)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--bitcoin-csv',
                       default='s3://bitcoin-bigdata/data/raw/Bitcoin_tweets.csv',
                       help='Ruta al CSV de Bitcoin en S3')
    parser.add_argument('--ethereum-csv',
                       default='s3://bitcoin-bigdata/data/raw/Ethereum_tweets.csv',
                       help='Ruta al CSV de Ethereum en S3')
    
    args = parser.parse_args()
    
    analyzer = BatchHistoricalAnalyzer()
    analyzer.run(args.bitcoin_csv, args.ethereum_csv)


if __name__ == "__main__":

    main()
