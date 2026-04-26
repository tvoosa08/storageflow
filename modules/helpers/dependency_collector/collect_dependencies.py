import glob
import hashlib
import json
import os
import shutil
import sys
import tempfile


def copy_files(
    source_path, included_patterns, excluded_patterns, temp_dir, target_sub_dir=None
):
    """Copy files from source_path to temp_dir (or temp_dir/target_sub_dir)
    based on include and exclude glob patterns.

    Args:
        source_path (str): The base directory from which to copy files.
        included_patterns (list): A list of glob patterns for files to include.
        excluded_patterns (list): A list of glob patterns for files to exclude.
        temp_dir (str): The root temporary directory where files will be staged.
        target_sub_dir (str, optional): An optional subdirectory within temp_dir
                                        where files for this dependency will be placed.
                                        If None, files are copied directly to temp_dir.
    """
    # Determine the effective base directory for copying files for this dependency
    if target_sub_dir:
        effective_dest_base = os.path.join(temp_dir, target_sub_dir)
    else:
        effective_dest_base = temp_dir

    # Ensure the effective destination base directory exists
    os.makedirs(effective_dest_base, exist_ok=True)

    # Step 1: Collect all files that match the included patterns from the source_path
    files_to_copy = []
    for pattern in included_patterns:
        # glob.glob needs to be applied relative to source_path
        # and then we'll calculate relative path from source_path to copy to effective_dest_base
        files_to_copy.extend(
            glob.glob(os.path.join(source_path, pattern), recursive=True)
        )

    # Step 2: Copy the matched files to effective_dest_base, maintaining directory structure
    for file_path in files_to_copy:
        # Skip directories in the list of matched files (glob might return directories)
        if os.path.isdir(file_path):
            continue

        # Calculate the relative path from the original source_path
        relative_path = os.path.relpath(file_path, source_path)
        dest_path = os.path.join(effective_dest_base, relative_path)
        dest_dir = os.path.dirname(dest_path)

        # Ensure the destination directory exists for the current file
        os.makedirs(dest_dir, exist_ok=True)

        # Copy the file, preserving metadata (like timestamps)
        shutil.copy2(file_path, dest_path)

    # Step 3: Remove any excluded files or directories from the effective_dest_base
    for pattern in excluded_patterns:
        # Apply exclusion patterns relative to the effective_dest_base
        excluded_paths = glob.glob(
            os.path.join(effective_dest_base, pattern), recursive=True
        )
        for excluded_path in excluded_paths:
            # Delete the file or directory
            if os.path.isdir(excluded_path):
                shutil.rmtree(excluded_path)
            else:
                os.remove(excluded_path)


def calculate_sha256(temp_dir):
    """Calculate the SHA256 hash of the files in the given directory.
    Files are processed in a consistent order to ensure reproducible hashes.

    Args:
        temp_dir (str): The root directory containing all staged files.

    Returns:
        str: The SHA256 hexadecimal digest of all files.
    """
    sha256_hash = hashlib.sha256()
    file_paths_to_hash = []

    # Walk through the directory to find all files (ignoring directories)
    # Collect all file paths first to sort them for consistent hashing
    for root, dirs, files in os.walk(temp_dir):
        # Sort files and directories to ensure consistent hash calculation across runs
        files.sort()
        # No need to sort dirs for hashing, but good practice for os.walk consistency
        # dirs.sort() # Not strictly necessary for hash, but can be added for overall predictability

        for file_name in files:
            file_paths_to_hash.append(os.path.join(root, file_name))

    # Sort the collected file paths to ensure consistent order for hashing
    file_paths_to_hash.sort()

    for file_path in file_paths_to_hash:
        with open(file_path, "rb") as f:
            # Read the file in chunks and update the hash
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


def main():
    # Read the input from Terraform
    input_json = sys.stdin.read()
    input_data = json.loads(input_json)

    dependencies = json.loads(input_data["dependencies"])

    # Create a temporary directory for staging files
    temp_dir = tempfile.mkdtemp(prefix="staging_dir_")

    # Loop through each dependency and copy the files
    for dep in dependencies:
        source_path = dep["source_path"]
        included_patterns = dep["included_patterns"]
        excluded_patterns = dep["excluded_patterns"]
        # Get the optional target_sub_dir for this dependency using .get()
        target_sub_dir = dep.get("target_sub_dir")

        # Copy the files from source_path to the temp_dir (or its subdirectory)
        copy_files(
            source_path, included_patterns, excluded_patterns, temp_dir, target_sub_dir
        )

    # Calculate the combined SHA256 hash of all copied files
    combined_sha256 = calculate_sha256(temp_dir)

    # Output the result as a JSON object
    output = {"combined_sha256": combined_sha256, "staging_dir": temp_dir}
    print(json.dumps(output))


if __name__ == "__main__":
    main()
