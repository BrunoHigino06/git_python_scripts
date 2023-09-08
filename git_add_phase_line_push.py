import os
import git
import fileinput

# Function to clone a repository to a specific branch
def clone_repository(repo_url, branch_name):
    # Name of the directory where the repository will be cloned (can be customized)
    repo_name = repo_url.split("/")[-1].replace(".git", "")
    
    # Full path to the repository directory
    repo_dir = os.path.join(os.getcwd(), repo_name)

    # Clone the repository
    repo = git.Repo.clone_from(repo_url, repo_dir, branch=branch_name)

    # Check if the branch exists
    if branch_name in repo.branches:
        # Checkout to the specified branch
        repo.git.checkout(branch_name)
        print(f"Clone and checkout on the branch '{branch_name}' and repository '{repo_name}'.")
        
        # Edit the main.tf file
        path_location = os.path.join(repo_dir, file_name)
        print(f"Start editing the file '{file_name}'.")
        add_phrase_to_line(path_location, line_number_to_add_phrase, phrase)

        # Add, commit, and push the changes
        add_commit_push(repo)

    else:
        print(f"The branch '{branch_name}' does not exist in the repository '{repo_name}'.")

# Function to add a phrase to a specific line of the main.tf file
def add_phrase_to_line(file_path, line_number, phrase_to_add):
    # Phrase to add
    phrase_to_add = phrase

    # Read the file content
    with open(file_path, "r") as file:
        lines = file.readlines()

    # Insert the phrase at the specified line
    if 1 <= line_number <= len(lines):
        lines.insert(line_number - 1, phrase_to_add + "\n")
    else:
        print(f"Invalid line number: {line_number}. The file has {len(lines)} lines.")

    # Write the updated content back to the file
    with open(file_path, "w") as file:
        file.writelines(lines)

# Function to add, commit, and push changes
def add_commit_push(repo):
    # Add all changes
    repo.git.add("--all")
    
    # Commit with a message
    commit_message = "Change made by script, add the enable_dns_hostnames = true parameter"
    repo.git.commit("-m", commit_message)
    
    # Push the changes
    repo.git.push()

# Name of the .txt file with the list of repository URLs
input_file = "change_file.txt"

# Name of the branch to which you want to clone the repositories
target_branch = "feature/weekend"

# File name to edit
file_name = ".\\terraform\\vpc.tf"

# phrase_to_add = "This is the new phrase added to line 8."
phrase = "  enable_dns_hostnames = true"

# Specify the line number where you want to add the phrase
line_number_to_add_phrase = 62

# Open the .txt file and clone the repositories
with open(input_file, "r") as file:
    for line in file:
        repo_url = line.strip()
        clone_repository(repo_url, target_branch)

print(f"Clone and phrase addition to {line_number_to_add_phrase} of {file_name} made with success")