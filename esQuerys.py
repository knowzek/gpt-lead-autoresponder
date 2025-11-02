# esQuerys.py
import os
from elasticsearch import Elasticsearch
from elasticsearch import AuthenticationException, TransportError
from dotenv import load_dotenv
load_dotenv()

ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "opportunities")

def _truthy(name: str, default=False) -> bool:
    v = os.getenv(name, None)
    if v is None:
        return default
    return str(v).lower() in ("1", "true", "yes", "on")

def _make_es_client():
    url = os.getenv("ELASTIC_URL")  # e.g. https://elastic.amserver.cloud:443
    user = os.getenv("ELASTIC_USERNAME")
    pwd  = os.getenv("ELASTIC_PASSWORD")
    if not (url and user and pwd):
        raise RuntimeError("Missing ELASTIC_URL/ELASTIC_USERNAME/ELASTIC_PASSWORD env vars.")

    # TLS settings
    verify_certs = _truthy("ELASTIC_VERIFY_CERTS", True)
    ca_cert = os.getenv("ELASTIC_CA_CERT")  # path to CA file if needed
    timeout = int(os.getenv("ELASTIC_TIMEOUT", "30"))

    client_kwargs = {
        "basic_auth": (user, pwd),
        "request_timeout": timeout,
        "verify_certs": verify_certs,
    }
    if ca_cert:
        client_kwargs["ca_certs"] = ca_cert

    # IMPORTANT: avoid trailing slash issues
    url = url.rstrip("/")

    return Elasticsearch(url, **client_kwargs)

esClient = _make_es_client()

def getNewDataByDate(date="2025-10-30"):
    """
    Return active docs updated or created on/after YYYY-MM-DD.
    """
    query = {
        "bool": {
            "should": [
                {"range": {"updated_at": {"gte": f"{date}T00:00:00"}}},
                {"range": {"created_at": {"gte": date, "format": "yyyy-MM-dd"}}},
            ],
            "minimum_should_match": 1,
            "filter": [{"term": {"isActive": True}}],
        }
    }
    try:
        res = esClient.search(index=ELASTIC_INDEX, query=query, size=1000)
        return res.get("hits", {}).get("hits", [])
    except AuthenticationException as e:
        print("❌ ES auth failed. Check ELASTIC_* env vars and endpoint allows Basic Auth.")
        print(str(e))
        return []
    except TransportError as e:
        print(f"❌ ES transport error: {e}")
        return []

def getNewData():
    query = {"bool": {"filter": [{"term": {"isActive": True}}]}}
    try:
        res = esClient.search(index=ELASTIC_INDEX, query=query, size=1000)
        return res.get("hits", {}).get("hits", [])
    except (AuthenticationException, TransportError) as e:
        print(f"❌ ES search error: {e}")
        return []

def getDocByID(doc_id, index=ELASTIC_INDEX):
    try:
        return esClient.get(index=index, id=doc_id)
    except Exception:
        return {"found": False}

def isIdExist(doc_id, index=ELASTIC_INDEX):
    try:
        return esClient.exists(index=index, id=doc_id)
    except Exception:
        return False

if __name__ == "__main__":
    print(isIdExist("test-id"))
