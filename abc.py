import requests
from requests.auth import HTTPBasicAuth

# ----------------------
# CONFIGURE THESE
# ----------------------
DREMIO_HOST = "http://<dremio-host>:9047"
USERNAME = "your_dremio_user"
PASSWORD = "your_dremio_password"

S3_BUCKET_NAME = "my-bucket"
S3_PATH = "sales_data"    # Path inside bucket (folder or file name)
DATA_FORMAT = "Parquet"   # Change to "JSON", "Text", etc. if needed
# ----------------------

# Build API endpoints
BY_PATH_URL = f"{DREMIO_HOST}/api/v3/catalog/by-path/s3/{S3_BUCKET_NAME}/{S3_PATH}"
CATALOG_URL = f"{DREMIO_HOST}/api/v3/catalog"

def get_dataset():
    """Check if dataset exists in Dremio catalog."""
    resp = requests.get(BY_PATH_URL, auth=HTTPBasicAuth(USERNAME, PASSWORD))
    if resp.status_code == 200:
        print(f"✅ Dataset exists: {S3_BUCKET_NAME}/{S3_PATH}")
        return resp.json()["id"]
    elif resp.status_code == 404:
        print(f"ℹ Dataset not found: {S3_BUCKET_NAME}/{S3_PATH}")
        return None
    else:
        resp.raise_for_status()

def create_dataset():
    """Create a new Physical Dataset in Dremio."""
    payload = {
        "entityType": "dataset",
        "path": ["s3", S3_BUCKET_NAME, S3_PATH],
        "type": "PHYSICAL_DATASET",
        "format": {"type": DATA_FORMAT}
    }
    resp = requests.post(CATALOG_URL, json=payload, auth=HTTPBasicAuth(USERNAME, PASSWORD))
    resp.raise_for_status()
    dataset_id = resp.json()["id"]
    print(f"✅ Created dataset: {S3_BUCKET_NAME}/{S3_PATH} (ID: {dataset_id})")
    return dataset_id

def refresh_metadata_async(dataset_id):
    """Trigger async metadata refresh for the dataset."""
    refresh_url = f"{CATALOG_URL}/{dataset_id}/refresh?async=true"
    resp = requests.post(refresh_url, auth=HTTPBasicAuth(USERNAME, PASSWORD))
    resp.raise_for_status()
    print(f"🔄 Triggered async metadata refresh for dataset {dataset_id}")

if __name__ == "__main__":
    dataset_id = get_dataset()
    if not dataset_id:
        dataset_id = create_dataset()
    refresh_metadata_async(dataset_id)
