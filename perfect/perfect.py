from prefect import flow, task
import boto3
import json
import time

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/pxg6af"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
UVA_ID = "pxg6af"
PLATFORM = "prefect"
TARGET_COUNT = 21

sqs = boto3.client("sqs", region_name="us-east-1")


def get_queue_counts():
    attrs = sqs.get_queue_attributes(
        QueueUrl=QUEUE_URL,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )
    a = attrs["Attributes"]
    visible = int(a.get("ApproximateNumberOfMessages", "0"))
    not_visible = int(a.get("ApproximateNumberOfMessagesNotVisible", "0"))
    delayed = int(a.get("ApproximateNumberOfMessagesDelayed", "0"))
    total_left = visible + not_visible + delayed
    return visible, not_visible, delayed, total_left


@task
def fetch_messages():
    collected = []
    max_loops = 200

    for i in range(max_loops):
        v, nv, d, total = get_queue_counts()
        print(f"[loop {i}] visible={v}, not_visible={nv}, delayed={d}, total_left={total}")
        if total == 0:
            print("Queue empty, stop fetching.")
            break

        resp = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            VisibilityTimeout=30,
            WaitTimeSeconds=10,
        )

        messages = resp.get("Messages", [])
        if not messages:
            print("No visible messages this round, waiting...")
            time.sleep(3)
            continue

        for m in messages:
            attrs = m.get("MessageAttributes", {})
            order_str = attrs.get("order_no", {}).get("StringValue")
            word = attrs.get("word", {}).get("StringValue")

            try:
                order_no = int(order_str) if order_str is not None else None
            except ValueError:
                order_no = None

            collected.append({"order_no": order_no, "word": word})

            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=m["ReceiptHandle"],
            )

            print(f"collected: order={order_no}, word={word}")

        time.sleep(1)

    print(f"Total collected: {len(collected)} messages")
    return collected


@task
def store_messages(msgs):
    with open("messages.json", "w") as f:
        json.dump(msgs, f, indent=2)
    print("messages.json written")


@task
def reassemble(msgs):
    valid = [m for m in msgs if m["order_no"] is not None and m["word"]]
    if len(valid) < TARGET_COUNT:
        raise ValueError(f"Only {len(valid)}/{TARGET_COUNT} messages collected.")

    ordered = sorted(valid, key=lambda x: x["order_no"])
    words = [m["word"] for m in ordered]
    text = " ".join(words)

    # small cleanup for punctuation spacing
    for p in [".", ",", "!", "?", ";", ":", ")", "]", "}"]:
        text = text.replace(f" {p}", p)
    for p in ["(", "[", "{"]:
        text = text.replace(f"{p} ", p)
    text = text.replace(" - ", "-")

    with open("full_message.txt", "w") as f:
        f.write(text + "\n")

    print("final phrase:", text)
    return text


@task
def send_solution(phrase: str):
    if not phrase or not phrase.strip():
        raise ValueError("Empty phrase, not submitting.")

    resp = sqs.send_message(
        QueueUrl=SUBMIT_QUEUE_URL,
        MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid": {
                "DataType": "String",
                "StringValue": UVA_ID,
            },
            "phrase": {
                "DataType": "String",
                "StringValue": phrase,
            },
            "platform": {
                "DataType": "String",
                "StringValue": PLATFORM,
            },
        },
    )
    status = resp["ResponseMetadata"]["HTTPStatusCode"]
    print("submit status:", status)
    return status


@flow
def sqs_pipeline():
    msgs = fetch_messages()
    store_messages(msgs)
    full = reassemble(msgs)
    send_solution(full)


if __name__ == "__main__":
    sqs_pipeline()
