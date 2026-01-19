import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

TABLE_NAME = os.environ["TABLE_NAME"]

ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Content-Type": "application/json; charset=utf-8",
}


def _json_default(o):
    if isinstance(o, Decimal):
        return int(o) if o % 1 == 0 else float(o)
    raise TypeError

def _response(status, body_obj):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body_obj, default=_json_default),
    }


def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    if method == "OPTIONS":
        return _response(200, {"ok": True})

    path_params = event.get("pathParameters") or {}
    store = path_params.get("store")

    if store:
        resp = table.query(KeyConditionExpression=Key("Store").eq(store))
        items = resp.get("Items", [])
        out = [{"store": i.get("Store"), "item": i.get("Item"), "count": i.get("Count")} for i in items]
        return _response(200, out)

    resp = table.scan()
    items = resp.get("Items", [])
    out = [{"store": i.get("Store"), "item": i.get("Item"), "count": i.get("Count")} for i in items]
    return _response(200, out)
