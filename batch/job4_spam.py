# batch/job4_spam.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os
import sys

class SpamAnalyzer:
    def __init__(self): 
        print("Analisis de Spam y Contenido Sospechoso")
        self.spark = SparkSession.builder \
            .appName("SpamAnalysis") \
            .config("spark.sql.shuffle.partitions", "3") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.hdfs_base = "s3://bitcoin-bigdata/analytics"
        self.hdfs_output = f"{self.hdfs_base}/batch_results"
                
        os.makedirs(f"{self.hdfs_output}/spam_analysis", exist_ok=True)
        
        self.spam_keywords = [
            "airdrop", "giveaway", "free", "join", "pump", "profit",
            "100x", "moonshot", "win", "offer", "exclusive", "bonus",
            "referral", "affiliate", "discount", "promo", "buy now",
            "limited time", "act now", "click here", "sign up"
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
            'text': '',
            'user_name': 'Unknown',
            'user_followers': 0
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
            'user_name': 'Unknown',
            'user_followers': 0
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
    
    def detect_urls(self, df):
        df_urls = df.withColumn(
            "contains_url", 
            col("text").rlike("http[s]?://")
        )
        return df_urls
    
    def detect_spam(self, df):
        spam_regex = "|".join([f"(?i){k}" for k in self.spam_keywords])
        
        df_spam = df.withColumn(
            "is_spam", 
            col("text").rlike(spam_regex)
        )
        return df_spam
    
    def detect_short_tweets(self, df):
        df_short = df.withColumn(
            "word_count", 
            size(split(col("text"), "\\s+"))
        ).withColumn(
            "is_very_short",
            when(col("word_count") <= 5, True).otherwise(False)
        )
        return df_short
    
    def analyze_urls(self, df):
        print("\n=== ANÁLISIS DE URLs ===")
        
        url_stats = df.groupBy("crypto_type", "contains_url") \
            .agg(count("*").alias("tweet_count")) \
            .orderBy("crypto_type", "contains_url")
        
        url_stats.show(truncate=False)
        
        url_pct = df.groupBy("crypto_type") \
            .agg(
                count("*").alias("total"),
                sum(when(col("contains_url") == True, 1).otherwise(0)).alias("with_url")
            ) \
            .withColumn("url_percentage", round((col("with_url") / col("total")) * 100, 2))
        
        print("\n=== Porcentaje de tweets con URLs ===")
        url_pct.show(truncate=False)
        
        return url_stats, url_pct
    
    def analyze_spam(self, df):
        print("\n=== ANÁLISIS DE SPAM PROMOCIONAL ===")
        
        spam_stats = df.groupBy("crypto_type", "is_spam") \
            .agg(count("*").alias("tweet_count")) \
            .orderBy("crypto_type", "is_spam")
        
        spam_stats.show(truncate=False)
        
        # Porcentajes
        spam_pct = df.groupBy("crypto_type") \
            .agg(
                count("*").alias("total"),
                sum(when(col("is_spam") == True, 1).otherwise(0)).alias("spam_count")
            ) \
            .withColumn("spam_percentage", round((col("spam_count") / col("total")) * 100, 2))
        
        print("\n=== Porcentaje de tweets spam ===")
        spam_pct.show(truncate=False)
        
        return spam_stats, spam_pct
    
    def analyze_short_tweets(self, df):
        """Analisis de tweets cortos"""
        print("\n=== ANALISIS DE TWEETS CORTOS (<=5 palabras) ===")
        
        short_stats = df.filter(col("is_very_short") == True) \
            .groupBy("crypto_type") \
            .agg(count("*").alias("very_short_count")) \
            .orderBy("crypto_type")
        
        short_stats.show(truncate=False)
        
        # Distribución de longitud
        length_dist = df.groupBy("crypto_type") \
            .agg(
                avg("word_count").alias("avg_words"),
                min("word_count").alias("min_words"),
                max("word_count").alias("max_words")
            )
        
        print("\n=== Estadísticas de longitud ===")
        length_dist.show(truncate=False)
        
        return short_stats, length_dist
    
    def analyze_combined_spam_indicators(self, df):
        """Analisis combinado: tweets con multiples indicadores de spam"""
        print("\n=== ANALISIS COMBINADO DE INDICADORES ===")
        
        df_scored = df.withColumn(
            "spam_score",
            (when(col("contains_url"), 1).otherwise(0) +
             when(col("is_spam"), 1).otherwise(0) +
             when(col("is_very_short"), 1).otherwise(0))
        )
        
        combined_stats = df_scored.groupBy("crypto_type", "spam_score") \
            .agg(count("*").alias("tweet_count")) \
            .orderBy("crypto_type", "spam_score")
        
        print("\n=== Distribución de spam score (0=limpio, 3=muy sospechoso) ===")
        combined_stats.show(truncate=False)
        
        high_risk = df_scored.filter(col("spam_score") >= 2) \
            .groupBy("crypto_type") \
            .agg(count("*").alias("high_risk_tweets"))
        
        print("\n=== Tweets de alto riesgo (score >= 2) ===")
        high_risk.show(truncate=False)
        
        return combined_stats, high_risk
    
    def generate_summary(self, df):
        print("\n=== RESUMEN GENERAL ===")
        
        summary = df.groupBy("crypto_type").agg(
            count("*").alias("total_tweets"),
            sum(when(col("contains_url"), 1).otherwise(0)).alias("with_urls"),
            sum(when(col("is_spam"), 1).otherwise(0)).alias("spam_tweets"),
            sum(when(col("is_very_short"), 1).otherwise(0)).alias("very_short"),
            avg("word_count").alias("avg_words")
        )
        
        summary.show(truncate=False)
        
        return summary
    
    def save_results(self, url_stats, spam_stats, short_stats, 
                     combined_stats, summary):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = f"{self.hdfs_output}/spam_analysis"
        
        print("\n=== Guardando resultados ===")
        
        print("Estadísticas de URLs")
        url_stats.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/url_stats_{timestamp}.csv")

        print("Estadísticas de spam...")
        spam_stats.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/spam_stats_{timestamp}.csv")

        print("Estadísticas de tweets cortos")
        short_stats.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/short_tweets_{timestamp}.csv")


        print("Análisis combinad")
        combined_stats.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/combined_spam_score_{timestamp}.csv")

        print("Resumen general...")
        summary.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/summary_{timestamp}.csv")

        print("Guardando Parquet")
        summary.write.mode("overwrite").parquet(f"{base_path}/summary_{timestamp}.parquet")
        
        print(f"\nResultados guardados en: {base_path}")
    
    def run(self, btc_csv, eth_csv):

        btc_df = self.read_bitcoin_csv(btc_csv)
        eth_df = self.read_ethereum_csv(eth_csv)
        combined_df = self.combine_datasets(btc_df, eth_df)
        
        df_clean = self.clean_data(combined_df)
        
        df_analyzed = df_clean
        df_analyzed = self.detect_urls(df_analyzed)
        df_analyzed = self.detect_spam(df_analyzed)
        df_analyzed = self.detect_short_tweets(df_analyzed)
        
        df_analyzed.cache()
        
        
        url_stats, url_pct = self.analyze_urls(df_analyzed)
        spam_stats, spam_pct = self.analyze_spam(df_analyzed)
        short_stats, length_dist = self.analyze_short_tweets(df_analyzed)
        
        combined_stats, high_risk = self.analyze_combined_spam_indicators(df_analyzed)
        
        summary = self.generate_summary(df_analyzed)

        self.save_results(url_stats, spam_stats, short_stats, 
                          combined_stats, summary)
        
        print("\nAnalisis completado exitosamente!")
        self.spark.stop()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Spam and Suspicious Content Analysis (Bitcoin + Ethereum) ',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--bitcoin-csv',
                       default='../data/raw/Bitcoin_tweets.csv',
                       help='Ruta al CSV de Bitcoin')
    parser.add_argument('--ethereum-csv',
                       default='../data/raw/Ethereum_tweets.csv',
                       help='Ruta al CSV de Ethereum')
    
    args = parser.parse_args()
    
    analyzer = SpamAnalyzer()
    analyzer.run(args.bitcoin_csv, args.ethereum_csv)


if __name__ == "__main__":
    main()