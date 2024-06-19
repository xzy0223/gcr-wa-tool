import argparse
import concurrent.futures
import csv
import io
import json
import logging
import re
import time
from io import StringIO
from urllib.parse import urlparse

import boto3
import tenacity
from botocore.exceptions import ClientError

# 配置日志记录器
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建 Athena 和 Bedrock 客户端
athena = boto3.client('athena')
bedrock = boto3.client("bedrock-runtime", region_name="us-west-2")
model_id = 'anthropic.claude-3-haiku-20240307-v1:0'

# 读取提示模板文件
with open('prompt.tpl', 'r', encoding='utf-8') as f:
    prompt_tpl = f.read()


def get_csv_header(athena_client, db_name, table_name):
    """
    从 Athena 表中获取 CSV 文件的表头, 用于enrich prompt模版
    
    :param athena_client: Athena 客户端对象
    :param db_name: 数据库名称
    :param table_name: 表名
    :return: 表头列表
    """
    response = athena_client.get_table_metadata(
        CatalogName='AwsDataCatalog',
        DatabaseName=db_name,
        TableName=table_name
    )
    columns = response['TableMetadata']['Columns']
    headers = [col['Name'] for col in columns]
    return headers


def get_resource_from_llm(bedrock_client, prompt, model_id="anthropic.claude-3-sonnet-20240229-v1:0"):
    """
    从 Bedrock 模型中获取资源信息
    
    :param bedrock_client: Bedrock 客户端对象
    :param prompt: 提示词
    :param model_id: 模型 ID
    :return: 模型返回的响应文本
    """
    # 定义retry装饰器, 防止bedrock出现异常
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

    # 将请求转换为 JSON 格式
    request = json.dumps(native_request)

    try:
        # 调用模型并获取响应
        response = invoke_model(model_id, request)

    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
        exit(1)

    # 解码响应体
    model_response = json.loads(response["body"].read())

    # 提取并返回响应文本
    response_text = model_response["content"][0]["text"]

    return response_text


def extract_resource(br_output):
    """
    从模型输出中提取资源信息
    
    :param br_output: 模型输出文本
    :return: 字段名service_name、服务名称结果、字段名resource、资源名称结果
    """
    service_name_regex = '<service_name>(.*?)</service_name>'
    resource_regex = '<resource>(.*?)</resource>'
    service_name_match = re.search(service_name_regex, br_output)
    resource_match = re.search(resource_regex, br_output)
    if service_name_match or resource_match:
        return 'service_name', service_name_match.group(1), 'resource', resource_match.group(1)
    else:
        return None


def process_row(row, bedrock_client, prompt_tpl, headers, refined_csv_header):
    """
    处理单行数据, 保留refined_csv_header范围里的columns
    并且让bedrock帮我程序分析P0/P1的finding中涉及的aws服务和资源，存储在service_name和resource字段中
    
    :param row: 原始行数据
    :param bedrock_client: Bedrock 客户端对象
    :param prompt_tpl: 提示词模板
    :param headers: 原始 CSV 表头，用于丰富提示词模版
    :param refined_csv_header: 精炼后的 CSV 表头
    :return: 处理后的行数据
    """
    refined_row = {}
    for field in refined_csv_header:
        if field != 'resource' and field != 'service_name':
            refined_row[field] = row[field]
        else:
            prompt = prompt_tpl.format(schema=headers, data=row.values())
            br_output = get_resource_from_llm(bedrock_client, prompt, model_id)
            service_name_field, service_name_result, resource_name_filed, resource_name_result = extract_resource(
                br_output)
            refined_row[service_name_field] = service_name_result if service_name_result else ''
            refined_row[resource_name_filed] = resource_name_result if resource_name_result else ''
    return refined_row


def generate_report(
        athena_client,
        db_name,
        table_name,
        athena_result_url,
        bedrock_client,
        prompt_tpl,
        headers,
        refined_report_header,
        refined_report_output_url
):
    """
    生成精炼后的报告, 只包括pillar,question,choice,ta check,service_name和resource
    
    :param athena_client: Athena 客户端对象
    :param db_name: 数据库名称
    :param table_name: 表名
    :param athena_result_url: Athena 查询结果的 S3 URL
    :param bedrock_client: Bedrock 客户端对象
    :param prompt_tpl: 提示模板
    :param headers: 原始 CSV 表头
    :param refined_report_header: 精炼后的报告表头
    :param refined_report_output_url: 精炼后的报告输出 S3 URL
    """
    s3 = boto3.resource('s3')

    # 执行 Athena 查询
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

    # 轮询查询状态，直到查询完成
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            break

        if query_execution_status == 'FAILED':
            raise Exception(f"Query execution failed: {query_status['QueryExecution']['Status']['StateChangeReason']}")

        # 等待一段时间后再次检查查询状态
        time.sleep(1)

    # 获取查询结果的 S3 路径
    response = athena.get_query_execution(QueryExecutionId=query_execution_id)
    result_s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
    parsed_url = urlparse(result_s3_path)
    result_bucket_name = parsed_url.netloc
    result_file_key = parsed_url.path.lstrip('/')

    # 读取查询结果文件
    s3 = boto3.resource('s3')
    result_obj = s3.Object(result_bucket_name, result_file_key)

    csv_buffer = StringIO()
    refined_csv_header = refined_report_header

    # 使用多线程处理查询结果, 保留需要的字段, 通过bedrock输出我们需要的额外字段数据
    with io.TextIOWrapper(result_obj.get()['Body'], encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        csv_writer = csv.DictWriter(csv_buffer, refined_csv_header)

        csv_writer.writeheader()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for row in csv_reader:
                future = executor.submit(process_row, row, bedrock_client, prompt_tpl, headers, refined_csv_header)
                futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                refined_row = future.result()
                print(refined_row)
                csv_writer.writerow(refined_row)

    # 上传精炼后的报告到 S3
    result_bucket_name, result_prefix = refined_report_output_url.strip('s3://').strip('/').split('/', 1)
    obj = s3.Object(result_bucket_name, result_prefix + '/refined-wa-ta-report.csv')
    obj.put(Body=csv_buffer.getvalue())


if __name__ == '__main__':
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='refine wa-ta-report')
    parser.add_argument('--db_name', required=True, help='db_name')
    parser.add_argument('--table_name', required=True, help='table_name')
    parser.add_argument('--athena_result_url', required=True, help='athena_result_url')
    parser.add_argument('--refined_report_output_url', required=True, help='refined_report_output_url')

    args = parser.parse_args()

    db_name = args.db_name
    table_name = args.table_name
    athena_result_url = args.athena_result_url
    refined_report_output_url = args.refined_report_output_url

    # 获取原始 CSV 表头
    headers = get_csv_header(athena, db_name, table_name)
    refined_report_header = ['pillar', 'question', 'choice', 'ta check', 'service_name', 'resource']

    # 生成精炼后的报告
    generate_report(athena, db_name, table_name, athena_result_url, bedrock, prompt_tpl, headers, refined_report_header,
                    refined_report_output_url)
