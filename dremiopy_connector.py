import pandas as pd
import numpy as np
import json
import requests
import time
from urllib.parse import quote

# https://gist.github.com/naren-dremio/8ab2f72342b3e94718756e367a9a448b
# https://www.dremio.com/resources/tutorials/using-the-rest-api/#toc_item_Prerequisites

class dremio_connector:

    def __init__(self, dremioServer):

        self.username = None
        self.password = None
        self.dremioServer = dremioServer
        self.headers = {'content-type':'application/json'}
        self.auth = False

    def login(self, username, password):

        self.username = username
        self.password = password

        loginData = {'userName': self.username, 'password': self.password}
        response = requests.post(f'{self.dremioServer}/apiv2/login', headers=self.headers, data=json.dumps(loginData))
        
        if response.status_code == 200:
            print ('Successfully authenticated.')

            data = json.loads(response.text)
            token = data['token']
            self.headers =  {'Authorization':f'_dremio{token}',
                             'Content-Type':'application/json'}
            self.auth = True

        else:
            print('Authentication failed.')
    
    def apiGet(self, endpoint):
        return json.loads(requests.get(f'{self.dremioServer}/api/v3/{endpoint}', headers=self.headers).text)

    def apiPost(self, endpoint, body=None):
        text = requests.post(f'{self.dremioServer}/api/v3/{endpoint}', headers=self.headers, data=json.dumps(body)).text

        # a post may return no data
        if (text):
            return json.loads(text)
        else:
            return None

    def apiPut(self, endpoint, body=None):
        return requests.put(f'{self.dremioServer}/api/v3/{endpoint}', headers=self.headers, data=json.dumps(body)).text

    def apiDelete(self, endpoint):
        return requests.delete(f'{self.dremioServer}/api/v3/{endpoint}', headers=self.headers)

    def getCatalogRoot(self):
        return self.apiGet('catalog')['data']

    def getByPathChildren(self, path, children, depth):
        # search children for the item we are looking for
        for item in children:
            if item['path'][depth] == path[0]:
                path.pop(0)
                response = self.apiGet('catalog/{id}'.format(id=quote(item['id'])))
                if len(path) == 0:
                    return response
                else:
                    return self.getByPathChildren(path, response['children'], depth + 1)

    def getByPath(self, path):
        # get the root catalog
        root = self.getCatalogRoot()

        for item in root:
            if item['path'][0] == path[0]:
                path.pop(0)

                if len(path) == 0:
                    return item
                else:
                    response = self.apiGet('catalog/{id}'.format(id=quote(item['id'])))
                    return self.getByPathChildren(path, response['children'], 1)
      
    def jobstatus(self, jobid):
        return self.apiGet(f'job/{jobid}')['jobState']

    def querySQL(self, query, request_limit = 4):
        if self.auth:
            queryResponse = self.apiPost('sql', body={'sql': query})
            jobid = queryResponse['id']

            while self.jobstatus(jobid) != 'COMPLETED':
                time.sleep(5)
            
            limit = 500 
            offset = 0
            results = self.apiGet(f'job/{jobid}/results?offset={offset}&limit={limit}')
            rowCount = results["rowCount"]
            result_df = pd.DataFrame.from_dict(results["rows"])
            request_count = int(np.ceil(rowCount/limit))
            for offset in range(1, request_count):
                results = self.apiGet(f'job/{jobid}/results?offset={(limit*offset)}&limit={limit}')
                result_temp = pd.DataFrame.from_dict(results["rows"])
                result_df = pd.concat([result_df, result_temp])

            return result_df
                        
        else:
            print("Error - Please login before running this command")

        