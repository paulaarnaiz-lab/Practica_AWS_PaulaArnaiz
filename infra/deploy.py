import os
import json
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]


def zip_lambda(src_dir: Path, out_zip: Path) -> None:
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    if out_zip.exists():
        out_zip.unlink()
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for p in src_dir.rglob("*"):
            if p.is_file():
                arcname = str(p.relative_to(src_dir)).replace("\\", "/")
                z.write(p, arcname=arcname)


def ensure_bucket(s3, bucket_name: str, region: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket_name)
        return
    except ClientError:
        pass

    kwargs = {"Bucket": bucket_name}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)


def ensure_ddb_table_with_stream(ddb, table_name: str) -> str:
    try:
        desc = ddb.describe_table(TableName=table_name)
        table = desc["Table"]
    except ddb.exceptions.ResourceNotFoundException:
        ddb.create_table(
            TableName=table_name,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "Store", "AttributeType": "S"},
                {"AttributeName": "Item", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "Store", "KeyType": "HASH"},
                {"AttributeName": "Item", "KeyType": "RANGE"},
            ],
            StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
        )
        ddb.get_waiter("table_exists").wait(TableName=table_name)
        table = ddb.describe_table(TableName=table_name)["Table"]

    stream_spec = table.get("StreamSpecification") or {}
    if not stream_spec.get("StreamEnabled"):
        ddb.update_table(
            TableName=table_name,
            StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
        )
        waiter = ddb.get_waiter("table_exists")
        waiter.wait(TableName=table_name)
        table = ddb.describe_table(TableName=table_name)["Table"]

    return table.get("LatestStreamArn")


def wait_lambda_ready(lambda_client, function_name: str, timeout_s: int = 120) -> None:
    t0 = time.time()
    while True:
        cfg = lambda_client.get_function_configuration(FunctionName=function_name)
        state = cfg.get("State", "Unknown")
        last_update = cfg.get("LastUpdateStatus", "Unknown")
        if state == "Active" and last_update in ("Successful", "Unknown"):
            return
        if time.time() - t0 > timeout_s:
            raise TimeoutError(
                f"Lambda {function_name} not ready after {timeout_s}s (State={state}, LastUpdateStatus={last_update})"
            )
        time.sleep(2)


def call_with_retries(fn, retries: int = 8, sleep_s: int = 2):
    last_exc = None
    for _ in range(retries):
        try:
            return fn()
        except ClientError as e:
            code = e.response["Error"].get("Code", "")
            if code in ("ResourceConflictException", "TooManyRequestsException"):
                last_exc = e
                time.sleep(sleep_s)
                continue
            raise
        except Exception as e:
            last_exc = e
            time.sleep(sleep_s)
    raise last_exc


def ensure_lambda(lambda_client, name: str, role_arn: str, zip_path: Path, env_vars: dict) -> str:
    code_bytes = zip_path.read_bytes()
    handler = "lambda_function.lambda_handler"
    runtime = "python3.11"

    def _get_arn():
        return lambda_client.get_function(FunctionName=name)["Configuration"]["FunctionArn"]

    try:
        lambda_client.get_function(FunctionName=name)
        wait_lambda_ready(lambda_client, name)

        call_with_retries(lambda: lambda_client.update_function_code(FunctionName=name, ZipFile=code_bytes, Publish=True))
        wait_lambda_ready(lambda_client, name)

        call_with_retries(
            lambda: lambda_client.update_function_configuration(
                FunctionName=name,
                Role=role_arn,
                Handler=handler,
                Runtime=runtime,
                Timeout=30,
                MemorySize=256,
                Environment={"Variables": env_vars},
            )
        )
        wait_lambda_ready(lambda_client, name)
        return _get_arn()

    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    resp = lambda_client.create_function(
        FunctionName=name,
        Role=role_arn,
        Runtime=runtime,
        Handler=handler,
        Code={"ZipFile": code_bytes},
        Timeout=30,
        MemorySize=256,
        Publish=True,
        Environment={"Variables": env_vars},
    )
    wait_lambda_ready(lambda_client, name)
    return resp["FunctionArn"]


def ensure_s3_trigger(s3, lambda_client, bucket: str, lambda_arn: str) -> None:
    stmt_id = f"s3invoke-{bucket}"
    try:
        lambda_client.add_permission(
            FunctionName=lambda_arn,
            StatementId=stmt_id,
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{bucket}",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise

    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {
                    "LambdaFunctionArn": lambda_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": ".csv"}]}},
                }
            ]
        },
    )


def ensure_http_api(apigw, lambda_client, api_name: str, lambda_arn: str, region: str, account_id: str):
    api_id = None
    for a in apigw.get_apis().get("Items", []):
        if a["Name"] == api_name:
            api_id = a["ApiId"]
            break

    if not api_id:
        api_id = apigw.create_api(
            Name=api_name,
            ProtocolType="HTTP",
            CorsConfiguration={
                "AllowOrigins": ["*"],
                "AllowMethods": ["GET", "OPTIONS"],
                "AllowHeaders": ["*"],
            },
        )["ApiId"]

    integrations = apigw.get_integrations(ApiId=api_id).get("Items", [])
    integration_id = None
    for it in integrations:
        if it.get("IntegrationUri") == lambda_arn:
            integration_id = it["IntegrationId"]
            break

    if not integration_id:
        integration_id = apigw.create_integration(
            ApiId=api_id,
            IntegrationType="AWS_PROXY",
            IntegrationUri=lambda_arn,
            PayloadFormatVersion="2.0",
        )["IntegrationId"]

    def ensure_route(route_key: str):
        for r in apigw.get_routes(ApiId=api_id).get("Items", []):
            if r["RouteKey"] == route_key:
                return
        apigw.create_route(ApiId=api_id, RouteKey=route_key, Target=f"integrations/{integration_id}")

    ensure_route("GET /items")
    ensure_route("GET /items/{store}")

    try:
        apigw.get_stage(ApiId=api_id, StageName="$default")
        apigw.update_stage(ApiId=api_id, StageName="$default", AutoDeploy=True)
    except ClientError:
        apigw.create_stage(ApiId=api_id, StageName="$default", AutoDeploy=True)

    stmt_id = f"apigw-{api_id}"
    try:
        lambda_client.add_permission(
            FunctionName=lambda_arn,
            StatementId=stmt_id,
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=f"arn:aws:execute-api:{region}:{account_id}:{api_id}/*/*/*",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise

    endpoint = apigw.get_api(ApiId=api_id)["ApiEndpoint"]
    return api_id, endpoint


def upload_dir(s3, bucket: str, local_dir: Path) -> None:
    if not local_dir.exists():
        raise RuntimeError(f"No existe la carpeta web: {local_dir}")

    for p in local_dir.rglob("*"):
        if not p.is_file():
            continue
        key = str(p.relative_to(local_dir)).replace("\\", "/")

        content_type = "application/octet-stream"
        if key.endswith(".html"):
            content_type = "text/html; charset=utf-8"
        elif key.endswith(".css"):
            content_type = "text/css; charset=utf-8"
        elif key.endswith(".js"):
            content_type = "application/javascript; charset=utf-8"

        s3.put_object(Bucket=bucket, Key=key, Body=p.read_bytes(), ContentType=content_type)


def presign(s3, bucket: str, key: str, expires_seconds: int = 3600) -> str:
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_seconds,
    )


def ensure_sns_topic_and_sub(sns, topic_name: str, email: str | None):
    topic_arn = sns.create_topic(Name=topic_name)["TopicArn"]

    if email:
        sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)

    return topic_arn


def ensure_event_source_mapping(lambda_client, function_arn: str, stream_arn: str):
    mappings = lambda_client.list_event_source_mappings(FunctionName=function_arn).get("EventSourceMappings", [])
    for m in mappings:
        if m.get("EventSourceArn") == stream_arn:
            return m["UUID"]

    resp = lambda_client.create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=function_arn,
        StartingPosition="LATEST",
        BatchSize=100,
        MaximumBatchingWindowInSeconds=1,
        Enabled=True,
    )
    return resp["UUID"]


def main():
    load_dotenv(ROOT / ".env")

    region = os.getenv("AWS_REGION", "us-east-1")
    suffix = os.environ["SUFFIX"]
    table_name = os.getenv("DDB_TABLE", "Inventory")

    lambda_role_arn = os.getenv("LAMBDA_ROLE_ARN")
    if not lambda_role_arn:
        raise RuntimeError("Falta LAMBDA_ROLE_ARN en .env (usa un role existente tipo LabRole).")

    notify_email = os.getenv("NOTIFY_EMAIL") or None
    threshold = os.getenv("LOW_STOCK_THRESHOLD", "2")

    uploads_bucket = f"inventory-uploads-{suffix}"
    web_bucket = f"inventory-web-{suffix}"

    session = boto3.session.Session(region_name=region)
    sts = session.client("sts")
    account_id = sts.get_caller_identity()["Account"]

    s3 = session.client("s3")
    ddb = session.client("dynamodb")
    lamb = session.client("lambda")
    apigw = session.client("apigatewayv2")
    sns = session.client("sns")

    print(f"Region: {region}")
    print(f"Suffix: {suffix}")
    print(f"Account: {account_id}")
    print(f"Lambda Role: {lambda_role_arn}")

    ensure_bucket(s3, uploads_bucket, region)
    ensure_bucket(s3, web_bucket, region)

    stream_arn = ensure_ddb_table_with_stream(ddb, table_name)
    if not stream_arn:
        raise RuntimeError("No se pudo obtener Stream ARN de DynamoDB (streams no activados).")

    build_dir = ROOT / "infra" / ".build"
    build_dir.mkdir(exist_ok=True)

    zip_a = build_dir / "load_inventory.zip"
    zip_b = build_dir / "get_inventory_api.zip"
    zip_c = build_dir / "notify_low_stock.zip"

    zip_lambda(ROOT / "lambdas" / "load_inventory", zip_a)
    zip_lambda(ROOT / "lambdas" / "get_inventory_api", zip_b)
    zip_lambda(ROOT / "lambdas" / "notify_low_stock", zip_c)

    fn_a = f"load_inventory_{suffix}"
    fn_b = f"get_inventory_api_{suffix}"
    fn_c = f"notify_low_stock_{suffix}"

    arn_a = ensure_lambda(lamb, fn_a, lambda_role_arn, zip_a, {"TABLE_NAME": table_name})
    arn_b = ensure_lambda(lamb, fn_b, lambda_role_arn, zip_b, {"TABLE_NAME": table_name})

    topic_name = f"inventory-low-stock-{suffix}"
    topic_arn = ensure_sns_topic_and_sub(sns, topic_name, notify_email)

    arn_c = ensure_lambda(
        lamb,
        fn_c,
        lambda_role_arn,
        zip_c,
        {"TOPIC_ARN": topic_arn, "THRESHOLD": str(threshold)},
    )

    ensure_s3_trigger(s3, lamb, uploads_bucket, arn_a)

    api_id, api_endpoint = ensure_http_api(apigw, lamb, f"inventory-api-{suffix}", arn_b, region, account_id)

    mapping_uuid = ensure_event_source_mapping(lamb, arn_c, stream_arn)

    upload_dir(s3, web_bucket, ROOT / "web")

    url_index = presign(s3, web_bucket, "index.html", expires_seconds=3600)

    print("\n=== DEPLOY OK ===")
    print("Uploads bucket:", uploads_bucket)
    print("Web bucket:   ", web_bucket)
    print("DDB table:    ", table_name)
    print("DDB stream:   ", stream_arn)
    print("SNS topic:    ", topic_arn)
    print("Stream->Lambda mapping UUID:", mapping_uuid)
    print("API endpoint: ", api_endpoint)
    print("Test API:     ", f"{api_endpoint}/items")
    print("Test store:   ", f"{api_endpoint}/items/Berlin")

    print("\nAbre la web (caduca en 1h).")
    print(url_index + "&api=" + api_endpoint)

    if notify_email:
        print("\nSNS email subscription:")
        print(" - Mira tu email y CONFIRMA la suscripción.")
    else:
        print("\nNOTIFY_EMAIL no configurado -> no se crea suscripción email.")

    print("\nSiguiente paso: subir un CSV a uploads bucket para poblar DynamoDB.")


if __name__ == "__main__":
    main()
