import requests
import time
import json

# beta env EC2
# url = "http://ec2-54-70-149-21.us-west-2.compute.amazonaws.com:9200"

# 2.11
#url = "http://ec2-18-237-133-156.us-west-2.compute.amazonaws.com:9200"

# nexus
# url = "http://ec2-54-70-136-129.us-west-2.compute.amazonaws.com:9200"

# repl
url = "http://ec2-52-39-211-201.us-west-2.compute.amazonaws.com:9200"

test_result = []

def enable_repl():
  async_url = url + "/_plugins/_query/settings"
  headers = {'Content-Type': 'application/json'}
  data = {"transient":{"plugins.query.executionengine.spark.session.enabled":"true"}}
  response = requests.put(async_url, headers=headers, json=data)
  if response.status_code // 100 == 2:
    # print(f"http request was successful (2xx status code).")
    return response
  else:
    print(f"http request failed with status code: {response.status_code}")
    raise Exception("FAILED")
  
def fetch_result(queryId):
  fetch_result_url = f"{url}/_plugins/_async_query/{queryId}"

  response = requests.get(fetch_result_url)
  if response.status_code // 100 == 2:
    # print(f"http request was successful (2xx status code).")
    return response
  else:
    print(f"http request failed with status code: {response.status_code}")
    raise Exception("FAILED")  

def asnyc_query(query):
  async_url = url + "/_plugins/_async_query"
  headers = {'Content-Type': 'application/json'}
  data = {
    "datasource": "mys3",
    "lang": "sql",
    "query": f"{query}"
  }
  response = requests.post(async_url, headers=headers, json=data)
  if response.status_code // 100 == 2:
    # print(f"http request was successful (2xx status code).")
    return response
  else:
    print(f"http request failed with status code: {response.status_code}")
    raise Exception("FAILED")  
  
def create_session():
  query = "select 1"
  print(f"\n======================")
  print(f"[{query}] START")
  print(f"======================")
  start_time = time.time()

  response=asnyc_query(query).json()
  sessionId = response['sessionId']
  queryId = response['queryId']
  print(f"sessionId: {sessionId}")
  while True:
    response = fetch_result(queryId).json()
    print(f"status: {response['status']}")
    if response['status'] == 'SUCCESS':
        query_end_time = time.time()
        print(f"\n======================")
        print(f"[{query}] SUCCESS")
        print(f"   Runtime {query_end_time - start_time} seconds")
        print(f"======================")
        return sessionId
    elif response['status'] == 'FAILED':
      raise Exception("FAILED")
    time.sleep(5)  

def asnyc_query_session(query, sessionId):
  async_url = url + "/_plugins/_async_query"
  headers = {'Content-Type': 'application/json'}
  data = {
    "datasource": "mys3",
    "lang": "sql",
    "query": f"{query}",
    "sessionId": f"{sessionId}"
  }
  response = requests.post(async_url, headers=headers, json=data)
  if response.status_code // 100 == 2:
    # print(f"http request was successful (2xx status code).")
    return response
  else:
    print(f"http request failed with status code: {response.status_code}")
    raise Exception("FAILED")   

def test_repl(expected, query, sessonId):
  print(f"\n========REPL==========")
  print(f"[{query}] START")
  print(f"======================")
  start_time = time.time()

  queryId = asnyc_query_session(query, sessonId).json()['queryId']
  print(f"queryId: {queryId}")
  while True:
    try:
      response = fetch_result(queryId).json()
      print(f"status: {response['status']}")
      if response['status'] == 'SUCCESS':
        query_end_time = time.time()
        if expected(response):
          print(f"\n======================")
          print(f"[{query}] SUCCESS")
          print(f"   Runtime {query_end_time - start_time} seconds")
          print(f"======================")  
          test_result.append(f"[{query}] SUCCESS. Runtime {query_end_time - start_time} seconds")
        else:
          print(json.dumps(response, indent=4))
          query_end_time = time.time()
          test_result.append(f"[{query}] FAILED. Runtime {query_end_time - start_time} seconds")
        break
      elif response['status'] == 'FAILED':
        print(f"{response['status']}")
        query_end_time = time.time()
        test_result.append(f"[{query}] FAILED. Runtime {query_end_time - start_time} seconds")
        break
    except Exception as e:
        print(f"{e}")
        query_end_time = time.time()
        test_result.append(f"[{query}] FAILED. Runtime {query_end_time - start_time} seconds")
        break
    time.sleep(2)


def main():
  enable_repl()
  sessionId = create_session()

  expected_lambda = lambda response: (
      response['status'] == 'SUCCESS' and
      response['total'] == 1 and
      response['datarows'][0] == [1998] and
      response['schema'][0]['name'] == 'year' and
      response['schema'][0]['type'] == 'integer'
  )
  test_repl(expected_lambda, "select year from mys3.default.http_logs where year = 1998 limit 1", sessionId)


  expected_lambda = lambda response: (
      response['size'] == 13 and
      response['total'] == 13 and
      response['datarows'][0] == [
        "@timestamp",
        "timestamp",
        ""
      ] and
      response['schema'] == [
        {
          "name": "col_name",
          "type": "string"
        },
        {
          "name": "data_type",
          "type": "string"
        },
        {
          "name": "comment",
          "type": "string"
        }
      ]
  )
  test_repl(expected_lambda, "DESC mys3.default.http_logs", sessionId)

  expected_lambda = lambda response: (
      response['size'] == 1 and
      response['total'] == 1 and
      response['datarows'][0] == [
        "default",
        "http_logs",
        False
      ] and
      response['schema'] == [
        {"name": "namespace", "type": "string"},
        {"name": "tableName", "type": "string"},
        {"name": "isTemporary", "type": "boolean"}
      ]
  )
  test_repl(expected_lambda, "SHOW TABLES IN mys3.default LIKE 'http_logs'", sessionId)


  expected_lambda = lambda response: (
      response['size'] == 0
  )
  test_repl(expected_lambda, "create skipping index on mys3.default.http_logs (status VALUE_SET)", sessionId)


  print(f"\n===========Summary=============")
  for item in test_result:
    print(item)
  print(f"===============================")    

if __name__ == '__main__':
    main()    
