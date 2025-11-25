
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import sys

class TemporalPatternAnalyzer:
    def __init__(self):
        print("JOB 3: ANALISIS DE PATRONES TEMPORALES")
        
        self.spark = SparkSession.builder \
            .appName("Job3_TemporalPatternAnalysis") \
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.s3_base = "s3://bitcoin-bigdata/analytics"
        self.s3_output = f"{self.s3_base}/batch_results/temporal_analysis"
        
        self.day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
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
        
        count = df.count()
        print(f"Bitcoin: {count:,} registros cargados")
        
        return df
    
    def read_ethereum_csv(self, csv_path):
        print(f"Leyendo Ethereum CSV: {csv_path}")
        
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        df = df.withColumn("crypto_type", lit("ethereum"))
        
        count = df.count()
        print(f"Ethereum: {count:,} registros cargados\n")
        
        return df
    
    def prepare_temporal_data(self, btc_df, eth_df):
        print("Preparando datos temporales...")
        
        combined_df = btc_df.select("date", "crypto_type").union(eth_df.select("date", "crypto_type"))
        
        time_df = combined_df \
            .withColumn("timestamp", to_timestamp(col("date"))) \
            .filter(col("timestamp").isNotNull()) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("day_of_week", date_format(col("timestamp"), "EEEE")) \
            .withColumn("date_only", to_date(col("timestamp")))
        
        time_df.cache()
        
        total = time_df.count()
        print(f"Total registros con timestamp valido: {total:,}\n")
        
        return time_df
    
    def analyze_hourly_activity(self, df):
        print("ANALISIS 1: ACTIVIDAD POR HORA DEL DIA")
        
        hourly_global = df.groupBy("hour") \
            .agg(
                count("*").alias("total_tweets")
            ) \
            .orderBy("hour")
        
        print("\nDistribucion global por hora (0-23):")
        hourly_global.show(24, truncate=False)
        
        hourly_by_crypto = df.groupBy("crypto_type", "hour") \
            .agg(
                count("*").alias("total_tweets")
            ) \
            .orderBy("crypto_type", "hour")
        
        print("\nDistribucion por cryptocurrency:")
        hourly_by_crypto.show(48, truncate=False)
        
        print("\nTop 5 horas mas activas:")
        top_hours = df.groupBy("hour") \
            .count() \
            .orderBy(desc("count")) \
            .limit(5)
        top_hours.show(truncate=False)
        
        return hourly_global, hourly_by_crypto
    
    def analyze_daily_activity(self, df):
        print("ANALISIS 2: ACTIVIDAD POR DIA DE LA SEMANA")
        
        daily_global = df.groupBy("day_of_week") \
            .agg(
                count("*").alias("total_tweets")
            ) \
            .orderBy(desc("total_tweets"))
        
        print("\nDistribucion global por dia:")
        daily_global.show(truncate=False)
        
        daily_by_crypto = df.groupBy("crypto_type", "day_of_week") \
            .agg(
                count("*").alias("total_tweets")
            ) \
            .orderBy("crypto_type", desc("total_tweets"))
        
        print("\nDistribucion por cryptocurrency:")
        daily_by_crypto.show(14, truncate=False)
        
        return daily_global, daily_by_crypto
    
    def analyze_heatmap_matrix(self, df):
        print("ANALISIS 3: MATRIZ DIA x HORA (HEATMAP)")
        
        heatmap_global = df.groupBy("day_of_week", "hour") \
            .agg(
                count("*").alias("total_tweets")
            ) \
            .orderBy(desc("total_tweets"))
        
        print("\nTop 30 combinaciones Dia-Hora mas activas:")
        heatmap_global.show(30, truncate=False)
        
        heatmap_by_crypto = df.groupBy("crypto_type", "day_of_week", "hour") \
            .agg(
                count("*").alias("total_tweets")
            ) \
            .orderBy("crypto_type", desc("total_tweets"))
        
        print("\nTop 20 por cryptocurrency:")
        heatmap_by_crypto.show(40, truncate=False)
        
        print("\nHora dorada identificada:")
        golden_hour = heatmap_global.limit(1)
        golden_hour.show(truncate=False)
        
        return heatmap_global, heatmap_by_crypto
    
    def analyze_weekend_vs_weekday(self, df):
        print("ANALISIS ADICIONAL: FIN DE SEMANA VS DIAS LABORALES")
        
        weekend_df = df.withColumn("is_weekend",
            when(col("day_of_week").isin(["Saturday", "Sunday"]), "Weekend")
            .otherwise("Weekday")
        )
        
        weekend_stats = weekend_df.groupBy("crypto_type", "is_weekend") \
            .agg(
                count("*").alias("total_tweets")
            ) \
            .orderBy("crypto_type", "is_weekend")
        
        print("\nComparacion Weekend vs Weekday:")
        weekend_stats.show(truncate=False)
        
        weekend_hourly = weekend_df.groupBy("is_weekend", "hour") \
            .agg(
                count("*").alias("total_tweets")
            ) \
            .orderBy("is_weekend", "hour")
        
        print("\nPatron horario en Weekend vs Weekday:")
        weekend_hourly.show(48, truncate=False)
        
        return weekend_stats
    
    def generate_summary(self, df):
        print("RESUMEN EJECUTIVO TEMPORAL")
        
        summary = df.groupBy("crypto_type") \
            .agg(
                count("*").alias("total_tweets"),
                countDistinct("date_only").alias("unique_days"),
                min("timestamp").alias("first_tweet"),
                max("timestamp").alias("last_tweet")
            )
        
        print("\n")
        summary.show(truncate=False)
        
        return summary
    
    def save_results(self, hourly_global, hourly_by_crypto, daily_global, daily_by_crypto, 
                    heatmap_global, heatmap_by_crypto, weekend_stats, summary):
        print("GUARDANDO RESULTADOS EN S3")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            print("\nGuardando analisis horario global...")
            hourly_global.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/hourly_global_{timestamp}.csv")
            
            print("Guardando analisis horario por crypto...")
            hourly_by_crypto.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/hourly_by_crypto_{timestamp}.csv")
            
            print("Guardando analisis diario global...")
            daily_global.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/daily_global_{timestamp}.csv")
            
            print("Guardando analisis diario por crypto...")
            daily_by_crypto.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/daily_by_crypto_{timestamp}.csv")
            
            print("Guardando matriz heatmap global...")
            heatmap_global.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/heatmap_global_{timestamp}.csv")
            
            print("Guardando matriz heatmap por crypto...")
            heatmap_by_crypto.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/heatmap_by_crypto_{timestamp}.csv")
            
            print("Guardando analisis weekend vs weekday...")
            weekend_stats.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/weekend_analysis_{timestamp}.csv")
            
            print("Guardando resumen ejecutivo...")
            summary.coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.s3_output}/temporal_summary_{timestamp}.csv")
            
            print("\nGuardando versiones Parquet...")
            heatmap_global.write.mode("overwrite").parquet(f"{self.s3_output}/parquet/heatmap_{timestamp}")
            hourly_by_crypto.write.mode("overwrite").parquet(f"{self.s3_output}/parquet/hourly_{timestamp}")
            daily_by_crypto.write.mode("overwrite").parquet(f"{self.s3_output}/parquet/daily_{timestamp}")
            
            print("\nTodos los resultados guardados exitosamente")
            print(f"Ubicacion base: {self.s3_output}/")
            
        except Exception as e:
            print(f"\nError al guardar resultados: {e}")
            import traceback
            traceback.print_exc()
    
    def run(self, btc_csv, eth_csv):
        try:
            btc_df = self.read_bitcoin_csv(btc_csv)
            eth_df = self.read_ethereum_csv(eth_csv)
            
            time_df = self.prepare_temporal_data(btc_df, eth_df)
            
            hourly_global, hourly_by_crypto = self.analyze_hourly_activity(time_df)
            daily_global, daily_by_crypto = self.analyze_daily_activity(time_df)
            heatmap_global, heatmap_by_crypto = self.analyze_heatmap_matrix(time_df)
            weekend_stats = self.analyze_weekend_vs_weekday(time_df)
            summary = self.generate_summary(time_df)
            
            self.save_results(hourly_global, hourly_by_crypto, daily_global, daily_by_crypto,
                            heatmap_global, heatmap_by_crypto, weekend_stats, summary)
            
            print("\n" + "="*80)
            print("ANALISIS TEMPORAL COMPLETADO EXITOSAMENTE")
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
        description='Job 3: Analisis de Patrones Temporales (Bitcoin + Ethereum)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--bitcoin-csv',
                       default='s3://bitcoin-bigdata/data/raw/Bitcoin_tweets.csv',
                       help='Ruta al CSV de Bitcoin en S3')
    parser.add_argument('--ethereum-csv',
                       default='s3://bitcoin-bigdata/data/raw/Ethereum_tweets.csv',
                       help='Ruta al CSV de Ethereum en S3')
    
    args = parser.parse_args()
    
    analyzer = TemporalPatternAnalyzer()
    analyzer.run(args.bitcoin_csv, args.ethereum_csv)


if __name__ == "__main__":

    main()
