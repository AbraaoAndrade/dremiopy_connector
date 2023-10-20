# dremiopy_connector

Dremio server connection by python

## Installation instructions 

```sh
pip install dremiopy_connector
```

## Usage instructions

```python

from dremiopy_connector import *
import credential

username = credential.username
password = credential.password

dremio_cnn = dremio_connector(dremioServer='http://server:port')
dremio_cnn.login(username, password)

query = """
SELECT *
FROM data
"""
data = dremio_cnn.querySQL(query, request_limit = 6)