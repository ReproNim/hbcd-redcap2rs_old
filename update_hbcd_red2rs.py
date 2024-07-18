import os
import shutil
import json
from git import Repo, exc as git_exc
import yaml
from subprocess import run, CalledProcessError

# Configurations
input_dir = 'path/input_dir'
yaml_file_path = 'path/hbcd-redcap2rs/hbcd_redcap2rs.yaml'
repo_path = 'path/hbcd-redcap2rs'  # Use the test repository path
dry_run = False  # Set this to False when you are confident in the script

# Initialize repository
repo = Repo(repo_path)

def get_latest_tag_version(repo, protocol_name):
    """Retrieve the latest tag version from the repository based on tag names and read the schema file."""
    tags = sorted(repo.tags, key=lambda t: t.name)
    
    # Print the sorted tag names for debugging purposes
    for tag in tags:
        print(f"Tag: {tag.name}")
    
    if tags:
        # The latest tag is the last one in the sorted list
        latest_tag = tags[-1]
        print(f"Latest Tag: {latest_tag.name}")  # Debugging print
        
        # Get the commit associated with the latest tag
        commit = latest_tag.commit
        
        # Retrieve the schema file content from the commit
        schema_file_path = f"{protocol_name}/{protocol_name}_schema"
        try:
            schema_content = (commit.tree / schema_file_path).data_stream.read().decode('utf-8')
            schema_data = json.loads(schema_content)
            version_str = schema_data.get('version', 'revid0')
            return int(version_str.replace('revid', ''))
        except KeyError:
            print(f"Schema file {schema_file_path} not found in the latest tag {latest_tag.name}")
            return 0
        except Exception as e:
            print(f"Error reading schema file {schema_file_path} from the latest tag {latest_tag.name}: {e}")
            return 0
    return 0

def update_repo(output_folder, folders_to_update, repo_path):
    """Move specific folders from output folder to the repository."""
    for folder in folders_to_update:
        dest_path = os.path.join(repo_path, folder)
        src_path = os.path.join(output_folder, folder)
        if dry_run:
            print(f"Would move {src_path} to {dest_path}")
        else:
            if os.path.exists(dest_path):
                shutil.rmtree(dest_path)  # Remove existing folder in destination (optional)
                print(f"Removed existing folder {dest_path}")
            try:
                os.rename(src_path, dest_path)  # Attempt atomic move
                print(f"Moved {src_path} to {dest_path} completed.")
            except OSError as e:
                print(f"Move failed for {src_path}: {e}")

def is_version_only_change(file_path, repo):
    """Check if the only change in the file is the 'version' field."""
    with open(file_path, 'r') as f:
        working_content = json.load(f)
    
    last_commit_content = repo.git.show(f'HEAD:{file_path}')
    last_commit_content = json.loads(last_commit_content)
    
    working_version = working_content.pop("version", None)
    last_commit_version = last_commit_content.pop("version", None)
    
    return working_content == last_commit_content and working_version != last_commit_version

def process_changed_files():
    """Process changed files and discard changes if only 'version' is modified."""
    repo.git.add(update=True)  # Ensure index is up to date
    for item in repo.index.diff(None):  # None means comparing working directory against HEAD
        if item.change_type == 'M' and item.a_path.endswith('.json'):
            file_path = os.path.join(repo_path, item.a_path)
            if is_version_only_change(file_path, repo):
                if dry_run:
                    print(f"Would checkout {file_path}")
                else:
                    repo.git.checkout(file_path)
                    print(f"Checkout {file_path}")
            else:
                print(f"{file_path} has more changes than version")

def commit_and_tag(commit_message, tag_name, tag_message, folders_to_update):
    """Commit changes and create a tag with a message, including only specified folders."""
    # Stage only specific folders
    for folder in folders_to_update:
        folder_path = os.path.join(repo_path, folder)
        if os.path.exists(folder_path):
            repo.git.add(folder_path)
        else:
            print(f"Folder {folder_path} does not exist and will not be added.")
    
    if dry_run:
        print(f"Would commit with message: {commit_message}")
        print(f"Would tag with name: {tag_name} and message: {tag_message}")
    else:
        repo.index.commit(commit_message)
        repo.create_tag(tag_name, message=tag_message)
        origin = repo.remote(name='origin')
        try:
            origin.push(tag_name)
            origin.push()
        except git_exc.GitCommandError:
            origin.push(force=True)
        print(f"***************************\nGit tagged {tag_name}\n***************************")

def main():
    absolute_dir = os.path.abspath(input_dir)
    
    with open(yaml_file_path, 'r') as f:
        yaml_content = yaml.safe_load(f)
    
    protocol_name = yaml_content['protocol_name']
    current_version = get_latest_tag_version(repo, protocol_name)
    
    # Determine sorting order based on current_version
    ascending_order = current_version == 0
    sorted_filenames = sorted(os.listdir(input_dir), reverse=not ascending_order)
    
    for filename in sorted_filenames:
        if filename.endswith('.csv'):
            parts = filename.split('_')
            date_time_str = parts[1] + '_' + parts[2]
            redcap_version = parts[-2].replace('revid', '')
            
            # Check only the latest version for subsequent runs
            if current_version == 0 or (int(redcap_version) > current_version):
                if int(redcap_version) >= 101:
                    yaml_content['redcap_version'] = f"revid{redcap_version}"
                    if dry_run:
                        print(f"Would update YAML with redcap_version: {yaml_content['redcap_version']}")
                    else:
                        with open(yaml_file_path, 'w') as f:
                            yaml.safe_dump(yaml_content, f)
                    
                    csv_file_path = os.path.join(input_dir, filename)
                    if dry_run:
                        print(f"Would convert {csv_file_path} using reproschema redcap2reproschema")
                    else:
                        command = [
                            "reproschema",
                            "redcap2reproschema",
                            csv_file_path,
                            yaml_file_path,
                            "--output-path",
                            absolute_dir
                        ]
                        try:
                            run(command, check=True)
                        except CalledProcessError as e:
                            print(f"Error running reproschema redcap2reproschema: {e}")
                            return
                    
                    output_folder = os.path.join(absolute_dir, protocol_name)
                    if dry_run:
                        print(f"Would validate {output_folder} using reproschema validate")
                    else:
                        try:
                            run(["reproschema", "validate", output_folder], check=True)
                        except CalledProcessError as e:
                            print(f"Error running reproschema validate: {e}")
                            return
                    
                    folders_to_update = [f"{protocol_name}", "activities"]  # Include only specific folders

                    update_repo(output_folder, folders_to_update, repo_path)
                    process_changed_files()
                    commit_message = f"converted hbcd redcap data dictionary {date_time_str} to reproschema"
                    tag_message = f"redcap data dictionary {date_time_str} to reproschema"
                    tag_name = date_time_str.replace("-", ".").replace("_", ".")
                    
                    commit_and_tag(commit_message, tag_name, tag_message, folders_to_update)
            else:
                print(f"No newer version detected or revid is not greater than 101 for file: {filename}")
                break  # Exit the loop if the latest version does not require an update

if __name__ == "__main__":
    main()