StorageFlow (SF) is an event-driven solution designed for transferring and transforming data from 
Google Cloud Storage (GCS) buckets to various GCP destinations like databases and 
other file systems. It reacts to GCS file modification events and routes them to actuators 
that perform data-related tasks.

Key functionalities include:

  * **Data Transformation**: Supports format and compression codec transformations.
  * **Data Ingestion**: Ingests data into managed locations and loads it into
  databases (BigQuery, Spanner, BigTable).
  * **Process Invocation**: Invokes processes such as workflows, Cloud Run functions, and
  task creation, leveraging Google Dataproc, Dataflow, Cloud Run, Cloud Batch, Cloud Tasks,
  and Cloud Workflows.

SF operates through several components:

  * **StorageFlow REST**: A Cloud Run-based API for event ingestion, task management, job execution,
  monitoring, logging, and error reporting.
  * **StorageFlow Controller**: A Cloud Workflow implementation for continuous job status reporting.
  * **StorageFlow Runtime**: Executes configured jobs using Dataproc and Workflow Templates, with
  current support for PySpark jobs.

Deployment and configuration are managed via Terraform modules. Jobs are defined with parameters 
such as `source_bucket`, `source_prefix` (regular expression for file paths), `mode` 
(FOLDER, MARKER, or FILE triggering), `job_type` (currently `pyspark`), and `cluster_blueprint_name`. 
Cluster blueprints define Dataproc cluster specifications.

Logging and error reporting are integrated with Google Cloud's Logging and Error Reporting suites, 
utilizing a `run_uuid` for consistent tracking. The solution requires specific 
Google APIs to be enabled and appropriate IAM permissions for its service accounts.
