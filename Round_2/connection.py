from elasticsearch import Elasticsearch

# Create an Elasticsearch connection
def create_es_connection():
    es_host = "http://127.0.0.1:8989"
    username = "elastic"
    password = "ETS5jjSF_js4vS*sZUJ*"
    
    es = Elasticsearch(
        es_host,
        basic_auth=(username, password),
        verify_certs=True,
    )
    
    # Test the connection by checking the cluster health
    try:
        health = es.cluster.health()
        print(f"\n[INFO] Elasticsearch connection successful. Cluster health: {health['status']}\n")
        return es
    except Exception as e:
        print(f"\n[ERROR] Failed to connect to Elasticsearch: {e}\n")
        return None
