import os
import shutil
import json
import yaml
from git import Repo, exc as git_exc
from subprocess import run, CalledProcessError

# Configurations
input_dir = './input_dir'
yaml_file_path = os.path.join(os.path.dirname(__file__), 'hbcd_redcap2rs.yaml')  # YAML file in the same directory
repo_path = os.getcwd()
version_threshold = 101  # Hardcoded threshold

def get_latest_tag_version(repo, protocol_name):
    """
    Get the latest version of the protocol schema from the repository tags.
    """
    tags = sorted(repo.tags, key=lambda t: t.name)
    if tags:
        latest_tag = tags[-1]
        commit = latest_tag.commit
        schema_file_path = f"{protocol_name}/{protocol_name}_schema"
        try:
            schema_content = (commit.tree / schema_file_path).data_stream.read().decode('utf-8')
            schema_data = json.loads(schema_content)
            version_str = schema_data.get('version', 'revid0')
            return int(version_str.replace('revid', ''))
        except Exception as e:
            print(f"Error reading schema file: {e}")
            return 0
    return 0

def update_repo(output_folder, folders_to_update, repo_path):
    """
    Update the repository with the converted reproschema output.
    """
    for folder in folders_to_update:
        dest_path = os.path.join(repo_path, folder)
        src_path = os.path.join(output_folder, folder)
        if os.path.exists(dest_path):
            shutil.rmtree(dest_path)
        shutil.move(src_path, dest_path)
        print(f"Moved {src_path} to {dest_path}")

def commit_and_tag(repo, commit_message, tag_name, tag_message, folders_to_update):
    """
    Commit the changes and create a new tag.
    """
    for folder in folders_to_update:
        folder_path = os.path.join(repo_path, folder)
        if os.path.exists(folder_path):
            repo.git.add(folder_path)
    repo.index.commit(commit_message)
    repo.create_tag(tag_name, message=tag_message)
    origin = repo.remote(name='origin')
    try:
        origin.push(tag_name)
        origin.push()
    except git_exc.GitCommandError:
        origin.push(force=True)
    print(f"Git tagged {tag_name}")

def convert_files(files_to_convert, protocol_name, current_version, dry_run=False):
    """
    Convert files one by one, processing them from the oldest to newest.
    """
    for file in files_to_convert:
        parts = file.split('_')
        date_time_str = parts[1] + '_' + parts[2]
        redcap_version = int(parts[-2].replace('revid', ''))

        if redcap_version > current_version:
            print(f"Processing version {redcap_version} (file: {file})...")

            # Dry-run simulation
            if dry_run:
                print(f"Dry-run mode: would process file {file} and update repo.")
                continue  # Skip actual processing during dry-run

            # Update YAML with the new version
            yaml_content['redcap_version'] = f"revid{redcap_version}"
            with open(yaml_file_path, 'w') as f:
                yaml.safe_dump(yaml_content, f)

            csv_file_path = os.path.join(input_dir, file)

            # Run reproschema conversion
            command = [
                "reproschema",
                "redcap2reproschema",
                csv_file_path,
                yaml_file_path,
                "--output-path",
                os.path.abspath(input_dir)
            ]
            try:
                run(command, check=True)
            except CalledProcessError as e:
                print(f"Error during reproschema conversion for {file}: {e}")
                return 1

            output_folder = os.path.join(os.path.abspath(input_dir), protocol_name)

            # Validate the reproschema output
            try:
                run(["reproschema", "validate", output_folder], check=True)
            except CalledProcessError as e:
                print(f"Error during reproschema validation for {file}: {e}")
                return 1

            # Update the repo and commit the changes
            folders_to_update = [f"{protocol_name}", "activities"]
            update_repo(output_folder, folders_to_update, repo_path)

            # Commit and tag the changes
            commit_message = f"converted hbcd redcap data dictionary {date_time_str} to reproschema"
            tag_message = f"redcap data dictionary {date_time_str} to reproschema"
            tag_name = date_time_str.replace("-", ".").replace("_", ".")
            commit_and_tag(repo, commit_message, tag_name, tag_message, folders_to_update)

    return 0

def main(dry_run=False):
    # Check if input directory exists
    if not os.path.exists(input_dir):
        print("Input directory not found!")
        return 1  # Exit with error code

    # Read YAML config file
    with open(yaml_file_path, 'r') as f:
        yaml_content = yaml.safe_load(f)

    protocol_name = yaml_content['protocol_name']
    repo = Repo(repo_path)
    current_version = get_latest_tag_version(repo, protocol_name)

    # Get all CSV files in the input directory
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    if not csv_files:
        print("No CSV files found in input directory.")
        return 1

    # Filter and sort files newer than the current version
    files_to_convert = sorted(
        [f for f in csv_files if int(f.split('_')[-2].replace('revid', '')) > current_version],
        key=lambda f: int(f.split('_')[-2].replace('revid', ''))
    )

    if not files_to_convert:
        print(f"No newer versions found. Current version is {current_version}.")
        return 0

    # Convert the filtered files from the oldest to the newest
    return convert_files(files_to_convert, protocol_name, current_version, dry_run)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Reproschema Converter Script")
    parser.add_argument("--dry-run", action="store_true", help="Run the script in dry-run mode")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
