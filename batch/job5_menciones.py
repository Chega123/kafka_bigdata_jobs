# batch/job5_menciones.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os
import sys

class MentionsAnalyzer:
    def __init__(self): 
        print("Análisis de menciones :D")

        self.spark = SparkSession.builder \
            .appName("MentionsAnalysis") \
            .config("spark.sql.shuffle.partitions", "3") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # HDFS
        self.hdfs_base = "s3://bitcoin-bigdata/analytics"
        self.hdfs_output = f"{self.hdfs_base}/batch_results"
                
        os.makedirs(f"{self.hdfs_output}/mentions", exist_ok=True)
        
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
            'text': '',
            'user_name': 'Unknown'
        })
        count = df.count()
        print(f"Bitcoin: {count:,} registros")
        
        return df
    
    def read_ethereum_csv(self, csv_path):
        print(f"Leyendo Ethereum CSV: {csv_path}")
        
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        df = df.withColumn("crypto_type", lit("ethereum"))
        df = df.fillna({
            'text': '',
            'user_name': 'Unknown'
        })
        count = df.count()
        print(f"Ethereum: {count:,} registros")
        
        return df
    
    def combine_datasets(self, btc_df, eth_df):
        combined_df = btc_df.union(eth_df)
        return combined_df
    
    def clean_data(self, df):
        cleaned_df = df \
            .filter(col("text").isNotNull()) \
            .filter(col("text") != "") \
            .filter(col("user_name").isNotNull()) \
            .withColumn("text", trim(col("text")))
        
        return cleaned_df
    
    def extract_mentions(self, df):
        df_mentions = df.withColumn(
            "mentions",
            expr("regexp_extract_all(text, '@([A-Za-z0-9_]+)')")
        )

        df_exploded = df_mentions.withColumn("mention", explode(col("mentions")))
        df_exploded = df_exploded.filter(col("mention") != "")
        
        return df_exploded
    
    def analyze_mentions_by_crypto(self, df):
        print("\n=== TOP MENCIONADOS EN BITCOIN ===")
        btc_mentions = df.filter(col("crypto_type") == "bitcoin") \
            .groupBy("mention") \
            .agg(count("*").alias("mention_count")) \
            .orderBy(col("mention_count").desc()) \
            .limit(20)
        
        btc_mentions.show(20, truncate=False)
        print("\n=== TOP MENCIONADOS EN ETHEREUM ===")
        eth_mentions = df.filter(col("crypto_type") == "ethereum") \
            .groupBy("mention") \
            .agg(count("*").alias("mention_count")) \
            .orderBy(col("mention_count").desc()) \
            .limit(20)
        
        eth_mentions.show(20, truncate=False)
        return btc_mentions, eth_mentions
    
    def analyze_global_mentions(self, df):
        print("\n=== TOP MENCIONADOS GLOBALMENTE ===")
        global_mentions = df.groupBy("mention") \
            .agg(count("*").alias("mention_count")) \
            .orderBy(col("mention_count").desc()) \
            .limit(30)
        
        global_mentions.show(30, truncate=False)
        
        return global_mentions
    
    def analyze_comparison(self, df):
        comparison = df.groupBy("mention") \
            .pivot("crypto_type") \
            .agg(count("*")) \
            .fillna(0) \
            .withColumn("total", col("bitcoin") + col("ethereum")) \
            .withColumn("bitcoin_pct", round((col("bitcoin") / col("total")) * 100, 2)) \
            .withColumn("ethereum_pct", round((col("ethereum") / col("total")) * 100, 2)) \
            .orderBy(col("total").desc()) \
            .limit(30)
        
        print("\n=== Top 30 menciones comparadas Bitcoin vs Ethereum ===")
        comparison.show(30, truncate=False)
        
        return comparison
    
    def generate_stats(self, df):
        stats = df.groupBy("crypto_type").agg(
            countDistinct("mention").alias("unique_mentions"),
            count("*").alias("total_mentions"),
            countDistinct("user_name").alias("users_mentioning")
        )
        
        print("\n=== Estadísticas por crypto ===")
        stats.show(truncate=False)
        
        return stats
    
    def save_results(self, btc_mentions, eth_mentions, global_mentions, comparison, stats):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = f"{self.hdfs_output}/mentions"
        
        print("\n=== Guardando resultados ===")
        
        print("Top menciones Bitcoin")
        btc_mentions.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/bitcoin_mentions_{timestamp}.csv")

        print("Top menciones Ethereum")
        eth_mentions.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/ethereum_mentions_{timestamp}.csv")

        print("Top menciones globales")
        global_mentions.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/global_mentions_{timestamp}.csv")

        print("Comparación Bitcoin vs Ethereum")
        comparison.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/comparison_{timestamp}.csv")

        print("Estadísticas generales")
        stats.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/stats_{timestamp}.csv")

        print("Guardando Parquet")
        global_mentions.write.mode("overwrite").parquet(f"{base_path}/global_mentions_{timestamp}.parquet")
        
        print(f"\n Resultados guardados en: {base_path}")
    
    def run(self, btc_csv, eth_csv):
        print("ANÁLISIS DE MENCIONES - BITCOIN & ETHEREUM")
        btc_df = self.read_bitcoin_csv(btc_csv)
        eth_df = self.read_ethereum_csv(eth_csv)
        combined_df = self.combine_datasets(btc_df, eth_df)

        df_clean = self.clean_data(combined_df)
        df_mentions = self.extract_mentions(df_clean)

        btc_mentions, eth_mentions = self.analyze_mentions_by_crypto(df_mentions)
        global_mentions = self.analyze_global_mentions(df_mentions)
        comparison = self.analyze_comparison(df_mentions)
        stats = self.generate_stats(df_mentions)

        self.save_results(btc_mentions, eth_mentions, global_mentions, comparison, stats)
        
        print("\n Análisis completado exitosamente!")
        self.spark.stop()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Mentions Analysis (Bitcoin + Ethereum)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--bitcoin-csv',
                       default='../data/raw/Bitcoin_tweets.csv',
                       help='Ruta al CSV de Bitcoin')
    parser.add_argument('--ethereum-csv',
                       default='../data/raw/Ethereum_tweets.csv',
                       help='Ruta al CSV de Ethereum')
    
    args = parser.parse_args()
    
    analyzer = MentionsAnalyzer()
    analyzer.run(args.bitcoin_csv, args.ethereum_csv)


if __name__ == "__main__":
    main()