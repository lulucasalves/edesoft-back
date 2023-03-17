import json
import boto3
import io
import csv
import datetime
import uuid


# import requests
def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("edesoftdb")

    currentTime = datetime.datetime.now()
    date = currentTime.strftime("%Y/%m/%d")

    if event["httpMethod"] == "GET":
        response = table.scan()
        return {
            "statusCode": 200,
            "body": json.dumps({"response": response}),
        }

    if event["httpMethod"] == "POST":
        body = json.loads(event["body"])
        name = body["bucket_name"]
        key = body["object_key"]

        s3Object = boto3.client("s3").get_object(Bucket=name, Key=key)
        s3Data = io.StringIO(s3Object["Body"].read().decode("ISO-8859-1"))
        s3DataReader = csv.reader(s3Data)

        for row in s3DataReader:
            for cell in row:
                if "-" in cell:
                    print(cell)
                    table.put_item(
                        Item={
                            "dynamo": {"S": uuid.uuid4()},
                            "cnpj/cpf": {
                                "S": cell.replace("/", "")
                                .replace(".", "")
                                .replace("-", "")
                            },
                            "date": {"S": date},
                        }
                    )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "success",
                }
            ),
        }
