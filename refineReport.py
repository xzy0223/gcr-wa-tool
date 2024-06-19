import boto3
import time

# 创建 Athena 客户端
athena = boto3.client('athena')

# 定义查询参数
params = {
    'region': 'us-west-2',
    'database': 'wa_tool_db',
    'bucket': 's3://aws-athena-query-results-us-west-2-935206693453',
    'path': 'path/to/query/result/',
    'query': 'SELECT * FROM wa_ta_report'
}

# 执行 Athena 查询
response = athena.start_query_execution(
    QueryString=params['query'],
    QueryExecutionContext={
        'Database': params['database']
    },
    ResultConfiguration={
        'OutputLocation': f"{params['bucket']}/{params['path']}"
    }
)

# 获取查询执行 ID
query_execution_id = response['QueryExecutionId']
print(query_execution_id)

# 等待查询完成
query_status = None
while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
    query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
    if query_status == 'FAILED' or query_status == 'CANCELLED':
        raise Exception('Athena query failed or was cancelled')
    time.sleep(1)

# 获取查询结果
results = athena.get_query_results(QueryExecutionId=query_execution_id)

# 遍历查询结果并打印每行数据
for row in results['ResultSet']['Rows']:
    print(row['Data'])
    break
