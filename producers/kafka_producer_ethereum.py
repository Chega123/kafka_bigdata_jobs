from kafka import KafkaProducer
import pandas as pd
import json
import time
from datetime import datetime
import sys
import os

class EthereumStreamSimulator:
    def __init__(self, csv_path, bootstrap_servers=['localhost:9092']):
        if not os.path.exists(csv_path):
            print(f"Error: No se encontro el archivo: {csv_path}")
            sys.exit(1)
        
        print(f"Dataset encontrado en: {csv_path}")
        self.csv_path = csv_path
        self.df = None
        
        print(f"Conectando a Kafka en {bootstrap_servers}")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                retries=5,
                acks='all',
                max_in_flight_requests_per_connection=5,
                compression_type='snappy'
            )
            print("Conectado a Kafka")
        except Exception as e:
            print(f"Error al conectar a Kafka: {e}")
            sys.exit(1)
        
        self.topic = 'ethereum-tweets'
        print(f"Topic configurado: {self.topic}")
        print()
    
    def load_and_clean_sample(self, num_rows_to_load):
        print(f"Cargando una muestra de {num_rows_to_load:,} filas de Ethereum")
        try:
            self.df = pd.read_csv(self.csv_path, nrows=num_rows_to_load, header=0)
            print(f"  Total de tweets en muestra: {len(self.df):,}")
        except Exception as e:
            print(f"Error al cargar el CSV: {e}")
            sys.exit(1)
        
        self.df = self.df.fillna({
            'user_name': 'Unknown', 
            'user_location': 'Unknown', 
            'user_description': '', 
            'user_followers': 0, 
            'user_friends': 0, 
            'user_favourites': 0, 
            'user_verified': False, 
            'text': '', 
            'hashtags': '', 
            'source': 'Unknown', 
            'is_retweet': False
        })
    
    def prepare_tweet(self, row):
        return {
            'crypto_type': 'ethereum',  
            'user_name': str(row['user_name']), 
            'user_location': str(row['user_location']), 
            'user_description': str(row['user_description']), 
            'user_created': str(row['user_created']), 
            'user_followers': int(row['user_followers']) if pd.notna(row['user_followers']) else 0, 
            'user_friends': int(row['user_friends']) if pd.notna(row['user_friends']) else 0, 
            'user_favourites': int(row['user_favourites']) if pd.notna(row['user_favourites']) else 0, 
            'user_verified': bool(row['user_verified']), 
            'date': str(row['date']), 
            'text': str(row['text']), 
            'hashtags': str(row['hashtags']), 
            'source': str(row['source']), 
            'is_retweet': bool(row['is_retweet']), 
            'timestamp': datetime.now().isoformat()
        }
    
    def start_streaming(self, tweets_per_minute=150, duration_minutes=10, sample_size=None):
        self.load_and_clean_sample(num_rows_to_load=100000)
        interval = 60 / tweets_per_minute
        if sample_size is None: 
            total_tweets = tweets_per_minute * duration_minutes
        else: 
            total_tweets = sample_size
        total_tweets = min(total_tweets, len(self.df))
        
        print("Iniciando streaming de ETHEREUM")
        sample_df = self.df.sample(n=total_tweets, random_state=42)
        tweets_sent = 0
        start_time = time.time()
        
        try:
            for idx, row in sample_df.iterrows():
                tweet_data = self.prepare_tweet(row)
                self.producer.send(self.topic, tweet_data)
                tweets_sent += 1
                
                if tweets_sent % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = tweets_sent / (elapsed / 60) if elapsed > 0 else 0
                    print(f"[ETH] {tweets_sent}/{total_tweets} - Rate: {rate:.1f} tweets/min")
                time.sleep(interval)
                
            self.producer.flush()
            print("\nETHEREUM streaming completado")
        except KeyboardInterrupt:
            print("\nStreaming detenido")
            self.producer.flush()
        except Exception as e:
            print(f"\nError: {e}")
            self.producer.flush()
    
    def close(self):
        if hasattr(self, 'producer'): 
            self.producer.close()

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', default='Ethereum_tweets.csv')
    parser.add_argument('--rate', type=int, default=150)
    parser.add_argument('--duration', type=int, default=10)
    parser.add_argument('--sample', type=int)
    parser.add_argument('--kafka', default='localhost:9092')
    parser.add_argument('--topic', default='ethereum-tweets')
    
    args = parser.parse_args()
    
    bootstrap_list = args.kafka.split(',') if ',' in args.kafka else [args.kafka]

    simulator = EthereumStreamSimulator(
        csv_path=args.csv,
        bootstrap_servers=bootstrap_list
    )
    if args.topic != 'ethereum-tweets': 
        simulator.topic = args.topic
    simulator.start_streaming(
        tweets_per_minute=args.rate, 
        duration_minutes=args.duration, 
        sample_size=args.sample
    )
    simulator.close()

if __name__ == "__main__":
    main()