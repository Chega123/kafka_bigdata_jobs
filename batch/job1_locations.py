# batch/job_locations_combined.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os
import sys

class CombinedLocationAnalyzer:
    def __init__(self): 
        print("Analisis de ubicaciones :D")

        # Configurar Spark
        self.spark = SparkSession.builder \
            .appName("CombinedLocationAnalysis") \
            .config("spark.sql.shuffle.partitions", "3") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        #HDFS
        self.hdfs_base = "s3://bitcoin-bigdata/analytics"
        self.hdfs_output = f"{self.hdfs_base}/batch_results"
                
        os.makedirs(f"{self.hdfs_output}/combined_locations", exist_ok=True)
        
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
            'user_location': 'Unknown',
            'user_followers': 0,
            'user_verified': False
        })
        count = df.count()
        print(f"Bitcoin: {count:,} registros")
        
        return df
    
    def read_ethereum_csv(self, csv_path):
        print(f"Leyendo Ethereum CSV: {csv_path}")
        
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        df = df.withColumn("crypto_type", lit("ethereum"))
        df = df.fillna({
            'user_location': 'Unknown',
            'user_followers': 0,
            'user_verified': False
        })
        count = df.count()
        print(f"Ethereum: {count:,} registros")
        
        return df
    
    def combine_datasets(self, btc_df, eth_df):
        combined_df = btc_df.union(eth_df)
        return combined_df
    
    def clean_locations(self, df):
        invalid_keywords = [
            'bitcoin', 'btc', 'eth', 'ethereum', 'crypto', 'nft', 'metaverse',
            'blockchain', 'moon', 'worldwide', 'global', 'earth', 'online',
            'internet', 'web', 'digital', 'virtual', 'defi', 'hodl', 'lambo',
            'satoshi', 'lightning', 'network', 'exchange', 'trading', 'wallet'
        ]  
        cleaned_df = df \
            .filter(col("user_location").isNotNull()) \
            .filter(col("user_location") != "") \
            .filter(col("user_location") != "Unknown") \
            .withColumn("location_clean", trim(lower(col("user_location")))) \
            .withColumn("location_clean", regexp_replace(col("location_clean"), "[📍🌍🌎🌏🚀💎]", "")) \
            .filter(~col("location_clean").startswith("["))  \
            .filter(~col("location_clean").startswith("#"))  \
            .filter(~col("location_clean").rlike("^\\d{4}-\\d{2}-\\d{2}"))  \
            .filter(~col("location_clean").rlike("^\\d{2}:\\d{2}:\\d{2}"))  \
            .filter(length(col("location_clean")) > 3) \
            .filter(length(col("location_clean")) < 100)
        
        for keyword in invalid_keywords:
            cleaned_df = cleaned_df.filter(~col("location_clean").contains(keyword))
        cleaned_df = cleaned_df.filter(~col("location_clean").rlike("^[0-9]+$"))
        cleaned_df = cleaned_df.filter(~col("location_clean").contains("http"))
        cleaned_df = cleaned_df.filter(~col("location_clean").contains("www."))      
        return cleaned_df
    
    def analyze_top_locations(self, df):
        top_by_crypto = df.groupBy("location_clean", "crypto_type") \
            .agg(
                count("*").alias("tweet_count"),
                countDistinct("user_name").alias("unique_users"),
                sum(when(col("user_verified") == True, 1).otherwise(0)).alias("verified_users"),
                avg("user_followers").alias("avg_followers")
            ) \
            .orderBy(col("tweet_count").desc())
        print("BITCOIN:")
        top_by_crypto.filter(col("crypto_type") == "bitcoin").show(20, truncate=False)
        print("ETHEREUM:")
        top_by_crypto.filter(col("crypto_type") == "ethereum").show(20, truncate=False)
        
        return top_by_crypto
    
    def analyze_comparison(self, df):
        comparison = df.groupBy("location_clean") \
            .pivot("crypto_type") \
            .agg(count("*")) \
            .fillna(0) \
            .withColumn("total", col("bitcoin") + col("ethereum")) \
            .withColumn("bitcoin_pct", round((col("bitcoin") / col("total")) * 100, 2)) \
            .withColumn("ethereum_pct", round((col("ethereum") / col("total")) * 100, 2)) \
            .orderBy(col("total").desc()) \
            .limit(30)
        
        print("Top 30 ubicaciones comparadas:")
        comparison.show(30, truncate=False)
        return comparison
    
    def generate_stats(self, df):
        stats = df.groupBy("crypto_type").agg(
            countDistinct("location_clean").alias("unique_locations"),
            count("*").alias("total_tweets"),
            countDistinct("user_name").alias("unique_users"),
            sum(when(col("user_verified") == True, 1).otherwise(0)).alias("verified_users"),
            avg("user_followers").alias("avg_followers")
        )
        print("Resumen por crypto:")
        stats.show(truncate=False)
        
        return stats
    
    def save_results(self, top_by_crypto, comparison, stats):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = f"{self.hdfs_output}/combined_locations"
        print("Top locations por crypto")
        top_by_crypto.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/top_by_crypto_{timestamp}.csv")

        print("Comparacion Bitcoin vs Ethereum")
        comparison.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/comparison_{timestamp}.csv")

        print("Estadistica general")
        stats.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/stats_{timestamp}.csv")

        print("Parquet")
        top_by_crypto.write.mode("overwrite").parquet(f"{base_path}/top_by_crypto_{timestamp}.parquet")
    
    def run(self, btc_csv, eth_csv):

        btc_df = self.read_bitcoin_csv(btc_csv)
        eth_df = self.read_ethereum_csv(eth_csv)
        combined_df = self.combine_datasets(btc_df, eth_df)
        
        df_clean = self.clean_locations(combined_df)

        top_by_crypto = self.analyze_top_locations(df_clean)
        comparison = self.analyze_comparison(df_clean)
        stats = self.generate_stats(df_clean)

        self.save_results(top_by_crypto, comparison, stats)
        
        self.spark.stop()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Combined Location Analysis (Bitcoin + Ethereum)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--bitcoin-csv',
                       default='../data/raw/Bitcoin_tweets.csv',
                       help='Ruta al CSV de Bitcoin')
    parser.add_argument('--ethereum-csv',
                       default='../data/raw/Ethereum_tweets.csv',
                       help='Ruta al CSV de Ethereum')
    
    args = parser.parse_args()
    
    analyzer = CombinedLocationAnalyzer()
    analyzer.run(args.bitcoin_csv, args.ethereum_csv)


if __name__ == "__main__":
    main()