from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
from datetime import datetime
import json


class ELKSender:
    def __init__(self, es_host='localhost', es_port=443, enabled=True, use_ssl=True, use_aws_auth=False, region='us-east-1'):
        self.enabled = enabled
        self.es_host = es_host
        self.es_port = es_port
        self.client = None
        
        if self.enabled:
            try:
                auth = None
                if use_aws_auth:
                    print(f" Configurando AWS Auth para región {region}...")
                    credentials = boto3.Session().get_credentials()
                    if credentials:
                        print(f"    Credenciales obtenidas: {credentials.access_key[:10]}...")
                        auth = AWS4Auth(
                            credentials.access_key,
                            credentials.secret_key,
                            region,
                            'es',
                            session_token=credentials.token
                        )
                    else:
                        print(f"    No se pudieron obtener credenciales AWS")
                        self.enabled = False
                        return
                
                clean_host = es_host.replace('https://', '').replace('http://', '')
                print(f" Conectando a {clean_host}:{es_port}...")
                
                self.client = OpenSearch(
                    hosts=[{'host': clean_host, 'port': es_port}],
                    http_auth=auth,
                    use_ssl=use_ssl,
                    verify_certs=True,
                    ssl_show_warn=False,
                    connection_class=RequestsHttpConnection,
                    timeout=30
                )
                
                print(f" Probando conexión...")
                if self.client.ping():
                    print(f" Conectado a OpenSearch exitosamente!")
                    info = self.client.info()
                    print(f"   Cluster: {info['cluster_name']}")
                    print(f"   Versión: {info['version']['number']}")
                else:
                    print(f" OpenSearch no responde al ping")
                    self.enabled = False
                    
            except Exception as e:
                print(f" Error al conectar a OpenSearch:")
                print(f"   Tipo: {type(e).__name__}")
                print(f"   Mensaje: {str(e)}")
                import traceback
                traceback.print_exc()
                print("   → Continuando sin OpenSearch...")
                self.enabled = False
        else:
            print("  OpenSearch Sender: Deshabilitado")
    
    def create_index(self, index_name, mapping):
        if not self.enabled:
            return False
        
        try:
            if not self.client.indices.exists(index=index_name):
                self.client.indices.create(index=index_name, body=mapping)
                print(f" Índice creado: {index_name}")
                return True
            else:
                print(f"  Índice ya existe: {index_name}")
            return True
        except Exception as e:
            print(f" Error al crear índice {index_name}: {e}")
            return False
    
    def send_document(self, index_name, document):
        if not self.enabled:
            return False
        
        try:
            self.client.index(index=index_name, body=document)
            return True
        except Exception as e:
            print(f" Error al enviar documento a {index_name}: {e}")
            return False
    
    def send_batch(self, index_name, documents):
        if not self.enabled or not documents:
            return 0
        
        count = 0
        for doc in documents:
            if self.send_document(index_name, doc):
                count += 1
        
        return count
    
    def send_batch_from_spark(self, batch_df, batch_id, index_name, transform_fn=None):
        if not self.enabled:
            return
        
        count = batch_df.count()
        if count == 0:
            return
        
        sent = 0
        for row in batch_df.collect():
            if transform_fn:
                doc = transform_fn(row)
            else:
                doc = row.asDict()
                for key, value in doc.items():
                    if hasattr(value, 'isoformat'):
                        doc[key] = value.isoformat()
            
            if self.send_document(index_name, doc):
                sent += 1
        
        print(f"[OpenSearch] Batch #{batch_id} - {sent}/{count} docs → '{index_name}'")


def create_elk_sender(enabled=True, es_host='localhost', es_port=443, use_aws_auth=False, region='us-east-1'):
    return ELKSender(
        es_host=es_host, 
        es_port=es_port, 
        enabled=enabled, 
        use_aws_auth=use_aws_auth,
        region=region
    )
