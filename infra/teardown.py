import os
import json
from dotenv import load_dotenv
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]


def empty_bucket(s3, bucket: str):
    # borra objetos y versiones si las hubiera
    try:
        paginator = s3.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket):
            objs = []
            for v in page.get("Versions", []):
                objs.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in page.get("DeleteMarkers", []):
                objs.append({"Key": m["Key"], "VersionId": m["VersionId"]})
            if objs:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
    except ClientError:
        # si no hay versiones habilitadas, cae aquí o no existe bucket
        pass

    # borra objetos normales
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
    except ClientError:
        pass


def main():
    load_dotenv(ROOT / ".env")

    region = os.getenv("AWS_REGION", "us-east-1")
    suffix = os.environ["SUFFIX"]
    table_name = os.getenv("DDB_TABLE", "Inventory")

    uploads_bucket = f"inventory-uploads-{suffix}"
    web_bucket = f"inventory-web-{suffix}"
    api_name = f"inventory-api-{suffix}"
    topic_name = f"inventory-low-stock-{suffix}"

    fn_a = f"load_inventory_{suffix}"
    fn_b = f"get_inventory_api_{suffix}"
    fn_c = f"notify_low_stock_{suffix}"

    session = boto3.session.Session(region_name=region)
    s3 = session.client("s3")
    ddb = session.client("dynamodb")
    lamb = session.client("lambda")
    apigw = session.client("apigatewayv2")
    sns = session.client("sns")

    print("== TEARDOWN ==")

    # 1) API Gateway: borrar API por nombre
    try:
        apis = apigw.get_apis().get("Items", [])
        for a in apis:
            if a.get("Name") == api_name:
                apigw.delete_api(ApiId=a["ApiId"])
                print("Deleted API:", a["ApiId"])
    except ClientError as e:
        print("API delete error:", e)

    # 2) Event source mappings + Lambdas
    for fn in [fn_a, fn_b, fn_c]:
        try:
            # borrar mappings (para C suele existir)
            mappings = lamb.list_event_source_mappings(FunctionName=fn).get("EventSourceMappings", [])
            for m in mappings:
                try:
                    lamb.delete_event_source_mapping(UUID=m["UUID"])
                    print("Deleted mapping:", m["UUID"])
                except ClientError:
                    pass

            lamb.delete_function(FunctionName=fn)
            print("Deleted Lambda:", fn)
        except ClientError:
            pass

    # 3) SNS topic por nombre
    try:
        topics = sns.list_topics().get("Topics", [])
        for t in topics:
            arn = t["TopicArn"]
            if arn.endswith(":" + topic_name):
                # borrar subscripciones
                subs = sns.list_subscriptions_by_topic(TopicArn=arn).get("Subscriptions", [])
                for s in subs:
                    if s.get("SubscriptionArn") and s["SubscriptionArn"] != "PendingConfirmation":
                        try:
                            sns.unsubscribe(SubscriptionArn=s["SubscriptionArn"])
                        except ClientError:
                            pass
                sns.delete_topic(TopicArn=arn)
                print("Deleted SNS topic:", arn)
    except ClientError as e:
        print("SNS delete error:", e)

    # 4) DynamoDB table
    try:
        ddb.delete_table(TableName=table_name)
        print("Deleted DynamoDB table:", table_name)
    except ClientError:
        pass

    # 5) Buckets (vaciar y borrar)
    for b in [uploads_bucket, web_bucket]:
        try:
            empty_bucket(s3, b)
            s3.delete_bucket(Bucket=b)
            print("Deleted bucket:", b)
        except ClientError:
            pass

    print("== DONE ==")


if __name__ == "__main__":
    main()

