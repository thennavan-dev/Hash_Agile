from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

# Create an index in Elasticsearch
def create_index(es, index_name):
    index_mapping = {
        "mappings": {
            "properties": {
                "Employee ID": {"type": "keyword"},
                "Full Name": {"type": "text"},
                "Job Title": {"type": "text"},
                "Department": {"type": "keyword"},
                "Business Unit": {"type": "text"},
                "Gender": {"type": "keyword"},
                "Ethnicity": {"type": "text"},
                "Age": {"type": "integer"},
                "Hire Date": {"type": "date"},
                "Annual Salary": {"type": "float"},
                "Bonus %": {"type": "float"},
                "Country": {"type": "text"},
                "City": {"type": "text"},
                "Exit Date": {"type": "date", "null_value": "null"}
            }
        }
    }

    if es.indices.exists(index=index_name):
        print(f"\n[INFO] Index '{index_name}' already exists. Skipping index creation.\n")
    else:
        es.indices.create(index=index_name, body=index_mapping)
        print(f"\n[INFO] Index '{index_name}' created successfully.\n")

def index_data(es, index_name, data):
    """Index data into Elasticsearch."""
    actions = [
        {
            "_op_type": "index",
            "_index": index_name,
            "_id": record["Employee ID"],
            "_source": record
        }
        for record in data
    ]
    
    try:
        # Bulk indexing
        success, failed = bulk(es, actions)
        print(f"\n[INFO] Successfully indexed {success} documents.")
        if failed > 0:
            print(f"[WARNING] {failed} document(s) failed to index. Please check the data.\n")
    except Exception as e:
        print(f"\n[ERROR] Failed to index data due to an error: {e}\n")

# Search for documents by a specific column and value
def search_by_column(es, index_name, column, value):
    query = {
        "query": {
            "match": {
                column: value
            }
        }
    }
    
    try:
        response = es.search(index=index_name, body=query)
        return response['hits']['hits']
    except Exception as e:
        print(f"\n[ERROR] Search failed: {e}\n")
        return []

# Delete an employee document by ID
def delete_employee(es, index_name, employee_id):
    try:
        es.delete(index=index_name, id=employee_id)
        print(f"\n[INFO] Employee with ID '{employee_id}' deleted successfully.\n")
    except NotFoundError:
        print(f"\n[ERROR] Employee with ID '{employee_id}' not found in Elasticsearch.\n")
    except Exception as e:
        print(f"\n[ERROR] Failed to delete employee with ID '{employee_id}': {e}\n")


# Function to count employee index
def get_employee_count(es, index_name):
    try:
        count = es.count(index=index_name)['count']
        print(f"\n[INFO] Total number of employees: {count}\n")
        return count
    except Exception as e:
        print(f"\n[ERROR] Failed to get employee count: {e}\n")
        return 0

# Function to get Department
def get_department_facets(es, index_name):
    query = {
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword",
                    "size": 10
                }
            }
        }
    }
    
    try:
        response = es.search(index=index_name, body=query)
        return response['aggregations']['departments']['buckets']
    except Exception as e:
        print(f"\n[ERROR] Failed to fetch department facets: {e}\n")
        return []
