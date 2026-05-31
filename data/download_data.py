import requests
import os

datasets = {
    "crimes.csv": "https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD",
    "police_stations.csv": "https://data.cityofchicago.org/api/views/z8bn-74gv/rows.csv?accessType=DOWNLOAD",
    "arrests.csv": "https://data.cityofchicago.org/api/views/dpt3-jri9/rows.csv?accessType=DOWNLOAD",
    "violence.csv": "https://data.cityofchicago.org/api/views/gumc-mgzr/rows.csv?accessType=DOWNLOAD",
    "sex_offenders.csv": "https://data.cityofchicago.org/api/views/vc9r-bqvy/rows.csv?accessType=DOWNLOAD"
}

data_dir = "d:/Big Data/crime_analytics/data"
os.makedirs(data_dir, exist_ok=True)

for name, url in datasets.items():
    print(f"Downloading {name}...")
    try:
        # For large files like crimes.csv, we only take the first 50,000 rows if it's too big
        # However, Chicago portal doesn't support easy 'top N' via rows.csv easily without SODA API
        # We will download the first chunk or just the whole file if it's manageable.
        # Given the 50k limit in prompt, let's try to limit if possible.
        
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(os.path.join(data_dir, name), 'wb') as f:
            count = 0
            for chunk in response.iter_lines():
                if count > 50000 and name == "crimes.csv":
                    break
                if count > 10000 and name != "crimes.csv":
                    break
                f.write(chunk + b'\n')
                count += 1
        print(f"Finished {name} ({count} rows).")
    except Exception as e:
        print(f"Error downloading {name}: {e}")
