from Round_2.csv_to_json import csv_to_json
from Round_2.connection import create_es_connection
from Round_2.elastic_functions import create_index, index_data, search_by_column, delete_employee, get_employee_count, get_department_facets

def main():
    csv_file = 'Employee_Sample_Data_1.csv'
    json_file = 'employee_data.json'
    data = csv_to_json(csv_file, json_file)
    
    if data is None:
        print("\n[ERROR] Failed to load data from CSV. Exiting...\n")
        return
    
    es = create_es_connection()
    if es is None:
        print("\n[ERROR] Failed to connect to Elasticsearch. Exiting...\n")
        return
    
    index_name = "hash_thennavan"
    create_index(es, index_name)
    
    index_data(es, index_name, data)
    
    department_results = search_by_column(es, index_name, "Department", "IT")
    if department_results:
        print(f"\n[INFO] Search results for Department 'IT':")
        for result in department_results:
            print(f"  - {result['_source']['Full Name']} (ID: {result['_source']['Employee ID']})")
    else:
        print("[INFO] No results found for Department 'IT'.")
    
    gender_results = search_by_column(es, index_name, "Gender", "Male")
    if gender_results:
        print(f"\n[INFO] Search results for Gender 'Male':")
        for result in gender_results:
            print(f"  - {result['_source']['Full Name']} (ID: {result['_source']['Employee ID']})")
    else:
        print("[INFO] No results found for Gender 'Male'.")
    
    department_facets = get_department_facets(es, index_name)
    if department_facets:
        print("\n[INFO] Department Facets:")
        for facet in department_facets:
            print(f"  - {facet['key']}: {facet['doc_count']} employees")
    else:
        print("[INFO] No department facets available.")
    
    get_employee_count(es, index_name)
    
    delete_employee(es, index_name, 'E02003')

if __name__ == "__main__":
    main()
