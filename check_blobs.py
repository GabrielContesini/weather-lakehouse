import json
from azure.storage.blob import BlobServiceClient

with open("local.settings.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

conn = cfg["Values"]["AZURE_STORAGE_CONNECTION_STRING"]
container = cfg["Values"].get("DATALAKE_CONTAINER", "datalake")

bsc = BlobServiceClient.from_connection_string(conn)
cc = bsc.get_container_client(container)

prefix = "bronze/openmeteo/dt=2026-02-25/"
items = [b.name for b in cc.list_blobs(name_starts_with=prefix)]

print("TOTAL:", len(items))
print("\n".join(items[:20]))
