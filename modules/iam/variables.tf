variable "project_id" {
  description = "GCP project ID where the storageflow service will be deployed"
  type        = string
}

variable "runtime_role_name" {
  description = "ID for the custom IAM role to be created"
  type        = string
  default     = "storageflow_runtime_role"
}

variable "runtime_role_permissions" {
  description = "Refined list of IAM permissions (avoiding admin roles) for the custom role."
  type        = list(string)
  default = [
    # --- General / Common Runtime Permissions ---
    "logging.logEntries.create",
    "resourcemanager.projects.get",
    "iam.serviceAccounts.actAs",
    "iam.serviceAccounts.getAccessToken",
    "iam.serviceAccounts.getOpenIdToken",

    # --- Dataproc ---
    "dataproc.clusters.create",
    "dataproc.clusters.get",
    "dataproc.clusters.list",
    "dataproc.jobs.create",
    "dataproc.jobs.get",
    "dataproc.jobs.list",
    "compute.instances.create",
    "compute.instances.setServiceAccount",
    "compute.subnetworks.use",
    "compute.networks.get",
    "compute.zones.list",
    "storage.objects.get", # For Dataproc workers/jobs reading from GCS
    "storage.objects.list",
    "storage.objects.create", # For Dataproc workers/jobs writing to GCS
    "storage.objects.delete",

    # --- Workflows ---
    "workflows.workflows.create",
    "workflows.workflows.update",
    "workflows.workflows.delete",
    "workflows.workflows.get",
    "workflows.workflows.list",

    # --- Cloud Tasks ---
    "cloudtasks.tasks.create",
    "cloudtasks.tasks.delete",
    "cloudtasks.tasks.get",
    "cloudtasks.tasks.list",
    "cloudtasks.tasks.run",
    "cloudtasks.queues.create", # If managing queues dynamically
    "cloudtasks.queues.delete",
    "cloudtasks.queues.get",
    "cloudtasks.queues.list",
    "cloudtasks.queues.update",

    # --- Cloud Scheduler ---
    "cloudscheduler.jobs.create",
    "cloudscheduler.jobs.delete",
    "cloudscheduler.jobs.get",
    "cloudscheduler.jobs.list",
    "cloudscheduler.jobs.update",
    "cloudscheduler.jobs.run",

    # --- Firebase (granular by product) ---
    "datastore.entities.create", # Cloud Firestore
    "datastore.entities.get",
    "datastore.entities.update",
    "datastore.entities.delete",
    "datastore.indexes.get",
    "datastore.indexes.list",
    "firebasedatabase.instances.get", # Realtime Database
    "firebasedatabase.instances.update",
    "firebaseauth.users.create", # Firebase Auth
    "firebaseauth.users.get",
    "firebaseauth.users.update",
    "firebaseauth.users.delete",

    # --- BigQuery ---
    "bigquery.jobs.create",
    "bigquery.jobs.get",
    "bigquery.jobs.list",
    "bigquery.tables.getData",
    "bigquery.tables.get",
    "bigquery.tables.list",
    "bigquery.tables.updateData",
    "bigquery.tables.create", # If creating tables dynamically
    "bigquery.tables.update",
    "bigquery.tables.delete",
    "bigquery.datasets.create", # If creating datasets dynamically
    "bigquery.datasets.update",
    "bigquery.datasets.delete",
    "bigquery.datasets.get",

    # --- Cloud Storage ---
    "storage.objects.get",
    "storage.objects.list",
    "storage.objects.create",
    "storage.objects.update",
    "storage.objects.delete",
    "storage.buckets.get",
    "storage.buckets.list",

    # --- Cloud Run ---
    "run.routes.invoke",   # If service account is client invoking Cloud Run
    "run.services.create", # If deploying/managing Cloud Run services
    "run.services.update",
    "run.services.delete",
    "run.services.get",
    "run.services.list",
    "run.revisions.list",
    "artifactregistry.repositories.uploadArtifacts",
    "artifactregistry.repositories.downloadArtifacts",
    "cloudbuild.builds.create",

    # --- Pub/Sub ---
    "pubsub.topics.publish",
    "pubsub.subscriptions.consume",
    "pubsub.topics.create", # If managing topics dynamically
    "pubsub.topics.delete",
    "pubsub.topics.get",
    "pubsub.topics.list",
    "pubsub.subscriptions.create", # If managing subscriptions dynamically
    "pubsub.subscriptions.delete",
    "pubsub.subscriptions.get",
    "pubsub.subscriptions.list",
    "pubsub.subscriptions.update",

    # --- Error Reporting ---
    "errorreporting.errorEvents.create",
    "errorreporting.errorEvents.list",
    "errorreporting.groups.list",
    "errorreporting.groupMetadata.get",
    "errorreporting.groupMetadata.update",
    "errorreporting.errorEvents.delete"
  ]
}
