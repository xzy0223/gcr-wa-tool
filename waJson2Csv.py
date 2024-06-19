import json
import csv
import boto3

from botocore.exceptions import ClientError

# Create a Bedrock Runtime client in the AWS Region of your choice.
client = boto3.client("bedrock-runtime", region_name="us-west-2")


def get_ta_checks(br_client, helpful_resource):
    # Set the model ID, e.g., Claude 3 Haiku.
    # model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    model_id = "anthropic.claude-3-sonnet-20240229-v1:0"

    # Define the prompt for the model.
    prompt_tpl = '''you are a skillful document reviewer. please help me pick the "trust advisor check" items from following text, must ignore the ramdon string in these items: \n
    {helpful_resource} \n
    instruction: \n
    1. extract items one by one  along with strictly outputing them delimited by comma. \n
    2. just output text without any redundant item like sign, sequence number and so on. \n
    '''

    prompt = prompt_tpl.format(helpful_resource=helpful_resource)

    # print(prompt)

    # Format the request payload using the model's native structure.
    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "temperature": 0.5,
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
    response_text = model_response["content"][0]["text"]
    print(response_text)

    # ta_checks = []
    # if helpful_resource:
    #     ta_checks = helpful_resource.split(" ")
    # return ta_checks

    return response_text



# 读取JSON数据
with open('/home/ec2-user/wa-tool/custom_lens.json', 'r') as file:
    data = json.load(file)

# 打开CSV文件并写入表头
with open('/home/ec2-user/wa-tool/output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Pillar', 'Question', 'Choice', 'TA Check'])

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
                    helpful_resource =get_ta_checks(client, choice['helpfulResource']['displayText'])

                    for ta_check in helpful_resource.split(","):
                        # 写入CSV文件
                        writer.writerow([pillar_name, question_title, choice_title, ta_check.strip()])