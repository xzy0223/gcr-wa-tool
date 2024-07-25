import boto3
import json
import csv
import argparse
import io

from urllib.parse import urlparse

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='format raw ta report')
    # 添加必需的命令行参数 raw_report_path，表示原始 TA 报告的本地路径
    parser.add_argument('--raw_report_path', required=True, help='input your local path of your raw TA report')
    # 添加必需的命令行参数 report_output_s3_path，表示格式化报告的目标 S3 路径
    parser.add_argument('--report_output_s3_path', required=True, help='input the destination s3 path you want to put your formatted report into')

    # 解析命令行参数
    args = parser.parse_args()

    # 获取原始报告路径和输出报告的 S3 路径
    raw_report_path = args.raw_report_path
    report_output_s3_path = args.report_output_s3_path

    # 从 S3 路径中解析出存储桶名称和对象键
    output_bucket_name = urlparse(report_output_s3_path, allow_fragments=False).netloc
    output_s3_key = urlparse(report_output_s3_path, allow_fragments=False).path[1:]

    # 创建 S3 资源对象
    s3 = boto3.resource('s3')
    output_obj = s3.Object(output_bucket_name, output_s3_key)

    # 创建一个内存中的字符串缓冲区，用于存储格式化后的 CSV 数据
    csv_buffer = io.StringIO()

    # 定义 CSV 文件的字段名
    fieldnames = ['account_id', 'region','check_id','check_name','category','status','reason','description']
    # 创建 CSV 字典写入器，并写入表头
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()

    # 打开原始 CSV 文件进行读取
    with open(raw_report_path, newline='') as rawcsvfile:
        # 创建 CSV 字典读取器
        reader = csv.DictReader(rawcsvfile)
        # 遍历原始 CSV 文件的每一行
        for row in reader:
            # 创建一个新的字典，用于存储格式化后的数据
            new_row = {}
            new_row['account_id']=row['Account ID']
            new_row['region']=row['Region']
            new_row['check_id']=row['Check ID']
            new_row['check_name']=row['Check Name']
            new_row['category']=row['Category']
            new_row['status']=''
            new_row['reason']=''

            # 解析 JSON 格式的 Properties.value 字段
            descriptions = json.loads(row['Properties.value'])
            list = []

            # 遍历 JSON 数据，提取状态、原因和描述信息
            for item in descriptions:
                if item['value'][0]=='Status: ':
                        new_row['status']=item['value'][1]
                elif item['value'][0]=='Reason: ':
                        new_row['reason']=item['value'][1]
                else:
                    list.append(''.join([str(value_item) for value_item in item['value']]))
            new_row['description'] = "; ".join(list)

            # 将格式化后的数据写入 CSV 缓冲区
            writer.writerow(new_row)
    
    # 将格式化后的 CSV 数据上传到 S3
    obj = s3.Object(output_bucket_name, output_s3_key)
    obj.put(Body=csv_buffer.getvalue())

    print('Your formatted TA report has been uploaded at {report_output_s3_path}'.format(report_output_s3_path=report_output_s3_path))

if __name__ == '__main__':
    main()