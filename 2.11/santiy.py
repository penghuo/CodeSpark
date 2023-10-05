import requests
import time

# beta env EC2
url = "http://ec2-54-70-149-21.us-west-2.compute.amazonaws.com:9200"

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

def fetch_result(queryId):
  fetch_result_url = f"{url}/_plugins/_async_query/{queryId}"

  response = requests.get(fetch_result_url)
  if response.status_code // 100 == 2:
    # print(f"http request was successful (2xx status code).")
    return response
  else:
    print(f"http request failed with status code: {response.status_code}")
    raise Exception("FAILED")

def test_query(expected, query):
  print(f"\n======================")
  print(f"[{query}] START")
  print(f"======================")
  start_time = time.time()

  queryId = asnyc_query(query).json()['queryId']
  print(f"queryId: {queryId}")
  while True:
    response = fetch_result(queryId).json()
    print(f"status: {response['status']}")
    if response['status'] == 'SUCCESS':
      if expected(response):
        query_end_time = time.time()
        print(f"\n======================")
        print(f"[{query}] SUCCESS")
        print(f"   Runtime {query_end_time - start_time} seconds")
        print(f"======================")
        
      else:
        raise Exception("FAILED")
      break
    elif response['status'] == 'FAILED':
      raise Exception("FAILED")
    time.sleep(1)

def main():
  expected_lambda = lambda response: (
      response['status'] == 'SUCCESS' and
      response['total'] == 1 and
      response['datarows'][0] == [1998] and
      response['schema'][0]['name'] == 'year' and
      response['schema'][0]['type'] == 'integer'
  )
  test_query(expected_lambda, "select year from mys3.default.http_logs where year = 1998 limit 1")


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
  test_query(expected_lambda, "DESC mys3.default.http_logs")

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
  test_query(expected_lambda, "SHOW TABLES IN mys3.default LIKE 'http_logs'")

if __name__ == '__main__':
    main()    
