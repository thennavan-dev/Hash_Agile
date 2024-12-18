import csv
import json

# Convert CSV to JSON and handle encoding errors
def csv_to_json(csv_file, json_file):
    try:
        # Try reading with ISO-8859-1 encoding first (handles special characters)
        with open(csv_file, mode='r', encoding='ISO-8859-1') as csvfile:
            reader = csv.DictReader(csvfile)
            data = [row for row in reader]

        with open(json_file, mode='w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=4)
        
        print(f"\n[INFO] CSV file '{csv_file}' successfully converted to JSON and saved as '{json_file}'.\n")
        return data
    except Exception as e:
        print(f"\n[ERROR] Failed to load CSV to JSON: {e}\n")
        return None
