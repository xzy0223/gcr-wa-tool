import json
import csv
import boto3
import argparse
import re

from botocore.exceptions import ClientError

# Create a Bedrock Runtime client in the AWS Region of your choice.
client = boto3.client("bedrock-runtime", region_name="us-west-2")


def get_ta_checks(br_client, ta_check):
    # Set the model ID, e.g., Claude 3 Haiku.
    # model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    model_id = "anthropic.claude-3-sonnet-20240229-v1:0"

    # Define the prompt for the model.
    prompt_tpl = '''you are a skillful document reviewer. please help me strip the non-meaningful string from the tail part of the following text: \n
    {ta_check} \n
    instruction: \n
    1. just output result without any redundant item like sign, sequence number and any other explaination. \n
    2. Remember, strip the non-meaningful random string, remain the meaningful words
    '''

    prompt = prompt_tpl.format(ta_check=ta_check)

    # print(prompt)

    # Format the request payload using the model's native structure.
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
        response = client.invoke_model(modelId=model_id, body=request)

    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
        exit(1)

    # Decode the response body.
    model_response = json.loads(response["body"].read())

    # Extract and print the response text.
    refined_ta_check = model_response["content"][0]["text"]
    print(f"{ta_check}\n{refined_ta_check}")

    return refined_ta_check

def get_ta_check_desc(search_ta, filename='./ta-check-desc.csv'):
    with open(filename, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if row[0] == search_ta:
                return row[1]
        return None

def main():

    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='generate wa json to ta check mapping table')
    # 添加必需的命令行参数 raw_report_path，表示原始 TA 报告的本地路径
    parser.add_argument('--wa_json_path', required=True, help='input your local path of your wa json')

    # 解析命令行参数
    args = parser.parse_args()

    wa_json_path = args.wa_json_path

    # 读取JSON数据
    with open(wa_json_path, 'r') as file:
        data = json.load(file)

    # 打开CSV文件并写入表头
    with open('./wa-issue-check.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['pillar', 'question', 'choice', 'ta_check', 'description'])

        # 遍历每个pillar
        for pillar in data['pillars']:
            pillar_name = pillar['name']

            # 遍历每个question
            for question in pillar['questions']:
                question_title = question['title']

                # 遍历每个choice
                for choice in question['choices']:
                    if "P0" in choice['title'] or "P1" in choice['title']:
                        
                        choice_title = choice['title']
                        raw_ta_checks = choice['helpfulResource']['displayText']

                        pattern = r"Trusted Advisor Checks:(.*?)Details:"
                        match = re.search(pattern, raw_ta_checks, re.DOTALL)
                        if match:
                            trimed_ta_checks = match.group(1).strip()
                            #print(trimed_ta_checks)
                        # helpful_resource = get_ta_checks(client, trimed_ta_checks).strip().strip(';')
                        
                        for ta_check in trimed_ta_checks.split("\n\n"):
                            if ta_check.strip() == None:
                                continue

                            refined_ta_ckeck = get_ta_checks(client, ta_check).strip()

                            description = get_ta_check_desc(refined_ta_ckeck.strip())
                            if description:
                                description = description.replace('\n', '\t\t')
                            # 写入CSV文件
                            writer.writerow([pillar_name, question_title, choice_title, refined_ta_ckeck, description])


if __name__ == '__main__':
    main()
    