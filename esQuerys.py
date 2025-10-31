from elasticsearch import Elasticsearch

# esClient = Elasticsearch(
#     "https://my-elasticsearch-project-d64bd4.es.us-central1.gcp.elastic.cloud:443",
#     api_key="Qm9LbDk1a0JqcG1uUl9iRjVrcUk6VUs2dW82eEZaQkE5OHd3aFZNeUxBUQ==",
# )

esClient = Elasticsearch(
    "https://elastic.amserver.cloud/",
    basic_auth=("elastic", "9*EhqLIs=pC8dlysUujK")
)

def getNewDataByDate(date = "2025-10-30"):
    query = {
        "bool": {
            "must": [
                {
                    "range": {
                        "created_at": {
                            "gte": date,
                            "format": "yyyy-MM-dd"
                        }
                    }
                }
            ]
        }
        
    }
    result = esClient.search(index="opportunities", query=query, size=1000)
    result = result['hits']['hits']
    return result


def getNewData():
    query = {
        "bool": {
            "must": [
                {
                    "term": {
                        "isActive": {
                            "value": True
                        }
                    }
                }
            ]
        }
    }
    result = esClient.search(index="opportunities", query=query, size=1000)
    result = result['hits']['hits']
    return result

def getDocByID(id, index= "opportunities"):
    try:
        return esClient.get(index=index, id=id)
    except:
        return {'found': False}
    
def isIdExist(id, index = "opportunities"):
    try:
        return esClient.exists(index = index, id = id)
    except:
        return False

if __name__ == "__main__":
    # doc = getDocByID(id = "0763b7ba-cdab-f011-814f-00505690ec8c")
    print(isIdExist("0763b7ba-cdab-f011-814f-00505690ec8"))