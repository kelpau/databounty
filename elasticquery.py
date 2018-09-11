import boto3
import json
from retrying import retry


@retry(stop_max_attempt_number=10, wait_fixed=2000)
def poll_status(client, id):
    '''
    poll query status
    '''
    print("poll")
    result = client.get_query_execution(
        QueryExecutionId=id
    )
    #print(result['QueryExecution'])
    state = result['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception


def main():

    with open('config_query.json') as f:
        data = json.load(f)

    if "database" not in data:
        print("No database in config file.")
        exit(1)
    
    if "query" not in data:
        print("No query in config file.")
        exit(1)

    if "output" not in data:
        print("No output in config file.")
        exit(1)

    

    client = boto3.client('athena')
    s3 = boto3.resource('s3')

    result = client.start_query_execution(
        QueryString=data["query"], QueryExecutionContext={'Database': data["database"]}, ResultConfiguration={'OutputLocation': data["output"]["s3"]})

    QueryExecutionId = result['QueryExecutionId']
    result = poll_status(client, QueryExecutionId)

    print(result)

    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = QueryExecutionId + '.csv'
        local_filename = data["output"]["local"] + '.csv'
        s3.Bucket("elasticscroll").download_file(s3_key, local_filename)


if __name__ == "__main__":
    main()
