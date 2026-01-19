import os
import csv
import io
import boto3

TABLE_NAME = os.environ["TABLE_NAME"]

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)


def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        return {"ok": True, "msg": "no records"}

    total_written = 0

    for r in records:
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8", errors="replace")

        reader = csv.DictReader(io.StringIO(body))
        with table.batch_writer(overwrite_by_pkeys=["Store", "Item"]) as batch:
            for row in reader:
                store = (row.get("store") or row.get("Store") or "").strip()
                item = (row.get("item") or row.get("Item") or "").strip()
                count_raw = (row.get("count") or row.get("Count") or "0").strip()

                if not store or not item:
                    continue

                try:
                    count = int(count_raw)
                except ValueError:
                    count = 0

                batch.put_item(Item={"Store": store, "Item": item, "Count": count})
                total_written += 1

    return {"ok": True, "written": total_written}

