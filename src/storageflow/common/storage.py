from google.cloud import storage
from google.api_core.exceptions import NotFound
import posixpath
from pathlib import Path
import ulid


def is_gcs_folder(
    bucket_name: str, prefix: str, client: storage.Client | None = None
) -> bool:
    """
    Determines if the GCS path is a folder (marker or prefix with children).
    Returns True if folder, False if file.
    Raises google.api_core.exceptions.NotFound if path doesn't exist.

    Args:
        bucket_name (str): GCS bucket name
        prefix (str): Object path or prefix
        client (storage.Client, optional): GCS client

    Returns:
        bool

    Raises:
        NotFound: if the path doesn't exist as file, folder, or marker
    """
    prefix = prefix.strip("/")
    client = client or storage.Client()
    bucket = client.bucket(bucket_name)

    if not prefix:
        return True  # bucket root is a valid folder

    file_blob = bucket.blob(prefix)
    if file_blob.exists():
        return False

    marker_blob = bucket.blob(f"{prefix}/")
    if marker_blob.exists():
        return True

    children = list(client.list_blobs(bucket, prefix=f"{prefix}/", max_results=1))
    if children:
        return True

    raise NotFound(f"GCS path not found: gs://{bucket_name}/{prefix}")


def strip_filename(gcs_path: str) -> str:
    """
    Removes the file name from a GCS-style path.

    Example:
        'some/folder/file.csv' -> 'some/folder/'
        'some/folder/'         -> 'some/folder/'

    Args:
        gcs_path (str): GCS path or prefix

    Returns:
        str: The path with file name stripped, ending in '/'
    """
    if gcs_path.endswith("/"):
        return gcs_path
    return posixpath.dirname(gcs_path).rstrip("/") + "/"


def upload_local_file(
    local_path: Path,
    job_name: str,
    bucket_name: str,
    client: storage.Client | None = None,
) -> str:
    """
    Uploads a local script to a GCS bucket under a content-addressed name.

    Args:
        local_path: Local path to the script file.
        job_name: Job name, used in the folder structure.
        bucket_name: GCS bucket name where the script will be uploaded.

    Returns:
        GCS URI as a string.
    """

    # Read file and hash content
    content = local_path.read_bytes()
    uid = ulid.ULID()
    original_name = local_path.name  # includes extension

    # Generate GCS object name
    object_name = f"staging/{job_name}/{uid}_{original_name}"

    # Upload to GCS
    client = client or storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    if not blob.exists():  # Avoid re-upload if already present
        blob.upload_from_string(content)

    return f"gs://{bucket_name}/{object_name}"
