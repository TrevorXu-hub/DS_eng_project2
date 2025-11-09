from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import boto3
import json
import time

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/pxg6af"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
UVA_ID = "pxg6af"
PLATFORM = "airflow"
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


def fetch_messages(**kwargs):
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
    # return via XCom
    return collected


def store_messages(**kwargs):
    ti = kwargs["ti"]
    msgs = ti.xcom_pull(task_ids="fetch_messages")
    with open("/opt/airflow/messages.json", "w") as f:
        json.dump(msgs, f, indent=2)
    print("messages.json written")


def reassemble(**kwargs):
    ti = kwargs["ti"]
    msgs = ti.xcom_pull(task_ids="fetch_messages")
    valid = [m for m in msgs if m["order_no"] is not None and m["word"]]

    if len(valid) < TARGET_COUNT:
        raise ValueError(f"Only {len(valid)}/{TARGET_COUNT} messages collected.")

    ordered = sorted(valid, key=lambda x: x["order_no"])
    words = [m["word"] for m in ordered]
    text = " ".join(words)

    for p in [".", ",", "!", "?", ";", ":", ")", "]", "}"]:
        text = text.replace(f" {p}", p)
    for p in ["(", "[", "{"]:
        text = text.replace(f"{p} ", p)
    text = text.replace(" - ", "-")

    with open("/opt/airflow/full_message.txt", "w") as f:
        f.write(text + "\n")

    print("final phrase:", text)
    return text  # XCom


def send_solution(**kwargs):
    ti = kwargs["ti"]
    phrase = ti.xcom_pull(task_ids="reassemble")
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


with DAG(
    dag_id="sqs_pipeline_airflow",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dp2", "sqs"],
) as dag:

    fetch_messages_task = PythonOperator(
        task_id="fetch_messages",
        python_callable=fetch_messages,
        provide_context=True,
    )

    store_messages_task = PythonOperator(
        task_id="store_messages",
        python_callable=store_messages,
        provide_context=True,
    )

    reassemble_task = PythonOperator(
        task_id="reassemble",
        python_callable=reassemble,
        provide_context=True,
    )

    send_solution_task = PythonOperator(
        task_id="send_solution",
        python_callable=send_solution,
        provide_context=True,
    )

    fetch_messages_task >> store_messages_task >> reassemble_task >> send_solution_task
