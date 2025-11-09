
# DS_Eng Project 2 — Prefect + AWS SQS

This project implements a batch-processing pipeline using Prefect to retrieve fragmented text messages from an instructor-provided AWS SQS queue, then reassemble them into a complete sentence and submit the final result to a designated submission queue. The workflow proceeds as follows: call the scatter API to generate 21 messages → poll the SQS queue until all messages (including delayed messages) are retrieved and deleted → store the raw fragments locally → reorder them by order_no and lightly clean spacing around punctuation → submit the reconstructed phrase to dp2-submit. The solution relies only on Prefect and boto3, keeping the implementation simple and reproducible.

### Project structure and dependencies

All source logic lives in prefect/sqs_pipeline_prefect.py, which contains SQS connection and retrieval, the Prefect flow (sqs_pipeline), reconstruction logic, and the submission step. Optional documentation is under docs/report.pdf, and sample screenshots are under screenshots/. The solution was tested on Python 3.11; dependencies (prefect, boto3) can be installed via pip install -r requirements.txt. AWS credentials must be available locally so boto3 can access the SQS queues.

### How to run

First, generate the 21 messages for your personal queue by calling the scatter API:
curl -X POST "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/pxg6af".
Next, from the project root, navigate to prefect/ and run python sqs_pipeline_prefect.py. Prefect will start a temporary local server and execute four tasks in order:
fetch_messages (poll and receive messages until none remain; delete each after processing),
store_messages (write fragments to messages.json),
reassemble (verify all 21 fragments, sort by order_no, join into a final sentence; perform minor punctuation cleanup; write full_message.txt), and
send_solution (send uvaid/phrase/platform as message attributes to https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit, expecting HTTP 200 on success).
Because messages are delayed by 30–900 seconds, manual waiting or automated polling is necessary; the script handles this via queue-attribute checks and repeated receive attempts.

<img width="483" height="146" alt="Screenshot 2025-11-09 at 22 38 30" src="https://github.com/user-attachments/assets/697db6c5-c8dc-4b5b-a6f6-0c3c731c645d" />


### Outputs and example

After execution, messages.json contains the full set of (order_no, word) fragments, and full_message.txt contains the reconstructed sentence.
An example output is:
Learning is not attained by chance, it must be sought for with ardor and attended to with diligence.-Abigal Adams
reassembled from 21 fragments indexed from 0 to 20.

<img width="1008" height="728" alt="Screenshot 2025-11-09 at 22 41 24" src="https://github.com/user-attachments/assets/b7b5f533-735f-43a5-822c-e4328512dd12" />

<img width="1052" height="185" alt="Screenshot 2025-11-09 at 22 41 34" src="https://github.com/user-attachments/assets/85babe07-a211-4a3f-b6a8-dfdde2fa97c6" />

### Design notes.

Prefect was chosen for its clean @flow/@task model and straightforward local execution and logging. The fetch_messages task inspects ApproximateNumberOfMessages, …NotVisible, and …Delayed to determine when the queue is truly empty, using long polling to reliably capture delayed messages. Each message is deleted right after processing to avoid leftovers. The reassemble task validates that all 21 fragments are present, sorts them, combines them, and minimally cleans punctuation spacing. Finally, send_solution submits the reconstructed phrase via SQS message attributes (uvaid/phrase/platform) and checks for HTTP 200. Together, these choices satisfy the assignment requirements for SQS retrieval, orchestrated execution, reconstruction, and submission.
