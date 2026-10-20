# 🎟️ Amusement Park Turnstile Engine

An asynchronous, fault-tolerant distributed worker system designed to process high-throughput event streams (ticket scans and turnstile access control) without missing events, double-processing, or failing during worker node crashes.

Built with **Python (boto3)**, **AWS SQS**, **AWS DynamoDB**, and provisioned via **Terraform (IaC)**.

---

## 🏛️ System Architecture

```text
                             +-------------------+
                             |   Turnstile Event |
                             |    (Producer)     |
                             +---------+---------+
                                       |
                                       v
                             +-------------------+
                             |  Amazon SQS Queue |
                             +---------+---------+
                                       |
                 +---------------------+---------------------+
                 |                     |                     |
                 v                     v                     v
         +---------------+     +---------------+     +---------------+
         | Worker Node 1 |     | Worker Node 2 |     | Worker Node N |
         +-------+-------+     +-------+-------+     +-------+-------+
                 |                     |                     |
                 +---------------------+---------------------+
                                       |
                                       v
                             +-------------------+
                             |  Amazon DynamoDB  |
                             |  (State Machine)  |
                             +-------------------+
                                       ^
                                       |
                             +---------+---------+
                             | Background Sweeper|
                             | (Self-Healing GC) |
                             +-------------------+
```

## ✨ Key Engineering Features

- **Atomic State Locking & Idempotency:** Leverages DynamoDB conditional expressions (`attribute_not_exists(ticket_id)`) to enforce single-worker execution and prevent duplicate ticket validation under network retry scenarios.
- **Active Heartbeat Threading:** While processing long-running jobs, worker nodes spawn a background daemon thread to continuously update a `last_heartbeat` timestamp in DynamoDB.
- **Self-Healing Garbage Collector (`sweeper.py`):** An isolated sweeper process routinely scans the database for jobs marked as `PROCESSING` whose heartbeats have stalled beyond a timeout threshold. Stale jobs are automatically reset to `PENDING` for re-execution.
- **Dead-Letter Queue (DLQ) Integration:** Failed or malformed messages are safely isolated in an SQS DLQ after exceeding maximum retry attempts, shielding main worker threads from poison-pill messages.
- **Automated Data Lifecycle (TTL):** Evaluates TTL parameters on job execution records to auto-expire historical logs from DynamoDB at zero additional operational cost.

---

## 📂 Project Structure

```text
.
├── main.tf               # Terraform HCL definitions (SQS, DLQ, DynamoDB, IAM)
├── producer.py           # Simulates batch turnstile scan events pushed to SQS
├── worker.py             # Core distributed worker with heartbeat daemon thread
├── sweeper.py            # Active/lazy expiration monitor for orphaned jobs
├── .gitignore            # Git exclusion rules for secrets, venv, and tfstate
└── .terraform.lock.hcl   # Terraform provider lock file
```

---

## 🧰 Tech Stack

| Category | Technology / Tool | Purpose |
| :--- | :--- | :--- |
| **Cloud & Messaging** | **Amazon SQS** | High-throughput message queuing and event buffering |
| **Database & State** | **Amazon DynamoDB** | Atomic job state tracking and conditional locking |
| **Infrastructure as Code** | **Terraform** | Provisioning and managing AWS cloud resources reproducibly |
| **Programming Language** | **Python 3.10+** | Producer, worker daemon, and sweeper logic implementation |
| **AWS SDK** | **Boto3** | Programmatic interaction with SQS and DynamoDB APIs |
| **Version Control** | **Git & GitHub** | Source code management and repository hosting |

---

## 🚀 Quickstart Guide

### 1. Prerequisites

- [Terraform](https://www.terraform.io/downloads) >= 1.0
- [Python](https://www.python.org/) 3.10+
- AWS CLI configured with appropriate permissions (`aws configure`)

### 2. Infrastructure Deployment

Initialize and provision the underlying AWS infrastructure using Infrastructure as Code:

```bash
# Initialize Terraform modules and providers
terraform init

# Inspect proposed AWS resource changes
terraform plan

# Deploy SQS queues and DynamoDB state table
terraform apply -auto-approve
```

### 3. Running the Engine Locally

Set up your local Python virtual environment:

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install required dependencies
pip install boto3
```

Run the individual engine components across separate terminal sessions:

```bash
# Terminal 1 — Start the Orphaned Task Sweeper
python sweeper.py

# Terminal 2 — Start the Event Worker Node
python worker.py

# Terminal 3 — Generate Turnstile Scan Events
python producer.py
```

---

## 🔄 State Machine Lifecycle

```text
    [ Event Produced ]
            │
            ▼
      ( SQS Queue )
            │
            ▼
       [ PENDING ] ────────► Worker acquires task lock (Sets worker_id & heartbeat)
            │
            ▼
      [ PROCESSING ] ◄─────► Background thread refreshes `last_heartbeat`
            │
      ┌─────┴────────────────────────┐
      │                              │
   (Success)                  (Worker Crash / Timeout)
      │                              │
      ▼                              ▼
 [ COMPLETED ]               [ SWEEPER RESCUE ]
                                     │
                                     ▼
                               [ PENDING ]
```

---

## 🧹 Cleanup

To teardown all active AWS cloud infrastructure and avoid unexpected charges:

```bash
terraform destroy -auto-approve
```

---


## 🗺️ Future Improvements & Roadmap

- **Containerization & Orchestration:** Package the worker and sweeper into Docker images and deploy them onto **Amazon ECS (Fargate)** or an **EKS Kubernetes cluster**.
- **Automated EC2 Provisioning:** Extend Terraform to automatically spin up target EC2 worker instances using user-data scripts or Ansible configuration management.
- **Observability & Metrics:** Integrate **Prometheus** metrics exporters and **Grafana** dashboards to monitor real-time queue depth, worker health, and throughput performance.
- **Dead-Letter Queue (DLQ) Alerting:** Set up Amazon CloudWatch alarms and SNS notifications whenever poison-pill messages are routed to the DLQ.



---


## 💡 Key Engineering Takeaways

- **Distributed Concurrency Control:** Gained deep hands-on experience handling race conditions across multiple worker nodes using atomic conditional writes (`attribute_not_exists`) rather than slow distributed locks.
- **Resiliency Through Heartbeats:** Learned how to implement self-healing background threads (`daemon=True`) to continuously renew leases on long-running tasks, ensuring crashed nodes never leave jobs locked forever.
- **Fault-Isolation Patterns:** Successfully decoupled event producers from worker execution speeds using SQS, ensuring downstream consumer failures don't bottleneck the entire ingestion pipeline.
- **Garbage Collection in Distributed Systems:** Designed an automated reconciliation loop (`sweeper.py`) to actively detect, quarantine, and recover orphaned or stalled workloads without human intervention.

