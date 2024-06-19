import boto3
import csv
import argparse
import json
import re
import time
from urllib.parse import urlparse
import io
from io import StringIO
import tenacity
import logging

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

athena = boto3.client('athena')
bedrock = boto3.client("bedrock-runtime", region_name="us-west-2")


with open('prompt.tpl', 'r', encoding='utf-8') as f:
    prompt_tpl = f.read()

def get_csv_header(athena_client, db_name, table_name):
    response = athena_client.get_table_metadata(
        CatalogName='AwsDataCatalog',
        DatabaseName=db_name,
        TableName=table_name
    )
    columns = response['TableMetadata']['Columns']
    headers = [col['Name'] for col in columns]
    return headers

def get_resource_from_llm(bedrock_client, prompt, model_id="anthropic.claude-3-sonnet-20240229-v1:0"):
    
    # 定义retry装饰器
    retry_invoke = tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_fixed(1),
        retry=(tenacity.retry_if_exception_type(ClientError) | 
           tenacity.retry_if_exception_type(Exception)),
        after=tenacity.after_log(logger, logging.WARNING)
        )
    
    # 将装饰器应用到调用函数上 
    @retry_invoke 
    def invoke_model(model_id, request):
        return bedrock_client.invoke_model(modelId=model_id, body=request)
    
    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "temperature": 0.1,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        ],
    }

    # Convert the native request to JSON.
    request = json.dumps(native_request)

    try:
        # Invoke the model with the request.
        response = invoke_model(model_id, request)

    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
        exit(1)

    # Decode the response body.
    model_response = json.loads(response["body"].read())

    # Extract and print the response text.
    response_text = model_response["content"][0]["text"]

    return response_text

def extract_resource(br_output):
    service_name_regex = '<service_name>(.*?)</service_name>'
    resource_regex = '<resource>(.*?)</resource>'
    service_name_match = re.search(service_name_regex, br_output)
    resource_match = re.search(resource_regex, br_output)
    if service_name_match or resource_match:
        return 'service_name', service_name_match.group(1), 'resource', resource_match.group(1)
    else:
        return None

def generate_report(
        athena_client, 
        db_name, 
        table_name,
        athena_result_url, 
        bedrock_client, 
        prompt_tpl, 
        headers,
        refined_report_header
        ):
    
    s3 = boto3.resource('s3')

    response = athena_client.start_query_execution(
        QueryString='SELECT * FROM {your_table_name}'.format(your_table_name=table_name),
        QueryExecutionContext={
            'Database': db_name
        },
        ResultConfiguration={
            'OutputLocation': athena_result_url
        }
    )
    query_execution_id = response['QueryExecutionId']
    
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            break

        if query_execution_status == 'FAILED':
            raise Exception(f"Query execution failed: {query_status['QueryExecution']['Status']['StateChangeReason']}")

        # Sleep for a short duration before checking again.
        time.sleep(1)
    
    response = athena.get_query_execution(QueryExecutionId=query_execution_id)
    result_s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
    parsed_url = urlparse(result_s3_path)
    result_bucket_name = parsed_url.netloc
    result_file_key = parsed_url.path.lstrip('/')
    
    s3 = boto3.resource('s3')
    result_obj = s3.Object(result_bucket_name, result_file_key)

    csv_buffer = StringIO()
    refined_csv_header = refined_report_header

    with io.TextIOWrapper(result_obj.get()['Body'], encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        csv_writer = csv.DictWriter(csv_buffer, refined_csv_header)

        csv_writer.writeheader()

        for row in csv_reader:
            refined_row = {}
            for field in refined_csv_header:
                if field != 'resource' and field != 'service_name':
                    refined_row[field] = row[field]
                else:
                    prompt = prompt_tpl.format(schema=headers, data=row.values())
                    br_output = get_resource_from_llm(bedrock_client, prompt)
                    service_name_field, service_name_result, resource_name_filed, resource_name_result = extract_resource(br_output)
                    refined_row[service_name_field] = service_name_result if service_name_result else ''
                    refined_row[resource_name_filed] = resource_name_result if resource_name_result else ''
            
            print(refined_row)
            csv_writer.writerow(refined_row)
            break

    obj = s3.Object(result_bucket_name, f'refined_report/refined_report.csv')
    obj.put(Body=csv_buffer.getvalue())



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='refine wa-ta-report')
    parser.add_argument('--db_name', required=True, help='db_name')
    parser.add_argument('--table_name', required=True, help='table_name')
    parser.add_argument('--athena_result_url', required=True, help='athena_result_url')

    args = parser.parse_args()
    
    db_name = args.db_name
    table_name = args.table_name
    athena_result_url = args.athena_result_url
    
    headers = get_csv_header(athena, db_name, table_name)
    refined_report_header = ['pillar', 'question', 'choice', 'ta check', 'service_name', 'resource']

    generate_report(athena, db_name, table_name, athena_result_url, bedrock, prompt_tpl, headers, refined_report_header)
