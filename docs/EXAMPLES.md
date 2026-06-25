# cloud_dog_vdb Examples

## Create a client and run a search
```python
from cloud_dog_vdb.factory import get_vdb_client

client = get_vdb_client(config)
results = await client.search("example query")
```
