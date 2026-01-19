import os
import json
import boto3

TOPIC_ARN = os.environ["TOPIC_ARN"]
THRESHOLD = int(os.getenv("THRESHOLD", "2"))

sns = boto3.client("sns")


def lambda_handler(event, context):
    records = event.get("Records", [])
    alerts = 0

    for r in records:
        if r.get("eventName") not in ("INSERT", "MODIFY"):
            continue

        new_img = (r.get("dynamodb") or {}).get("NewImage")
        if not new_img:
            continue

        store = (new_img.get("Store") or {}).get("S")
        item = (new_img.get("Item") or {}).get("S")
        count_str = (new_img.get("Count") or {}).get("N")

        if store is None or item is None or count_str is None:
            continue

        try:
            count = int(count_str)
        except ValueError:
            continue

        if count <= THRESHOLD:
            msg = {
                "type": "LOW_STOCK",
                "store": store,
                "item": item,
                "count": count,
                "threshold": THRESHOLD,
            }
            sns.publish(
                TopicArn=TOPIC_ARN,
                Subject=f"Low stock: {store} - {item}",
                Message=json.dumps(msg, ensure_ascii=False),
            )
            alerts += 1

    return {"ok": True, "alerts_sent": alerts}
