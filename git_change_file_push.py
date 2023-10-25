# This script automates the process of cloning Git repositories, checking out a specific branch, and
# editing a 'main.tf' file in those repositories. It replaces occurrences of 'word_find' with 'word_replace' in 'main.tf'.
# After editing, the script commits and pushes the changes to the repository. The list of repository URLs is read from 'change_file.txt'.

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
        edit_main_tf(path_location)

        # Add, commit, and push the changes
        add_commit_push(repo)

    else:
        print(f"The branch '{branch_name}' does not exist in the repository '{repo_name}'.")

# Function to edit the main.tf file
def edit_main_tf(file_path):
    # Open the main.tf file for editing
    with fileinput.FileInput(file_path, inplace=True) as file:
        for line in file:
            # Replace "word_find" with "word_replace" in each line
            line = line.replace(word_find, word_replace)
            print(line, end='')

# Function to add, commit, and push changes
def add_commit_push(repo):
    # Add all changes
    repo.git.add("--all")
    
    # Commit with a message
    commit_message = "Change made by script"
    repo.git.commit("-m", commit_message)
    
    # Push the changes
    repo.git.push()

# Name of the .txt file with the list of repository URLs
input_file = "change_file.txt"

# Name of the branch to which you want to clone the repositories
target_branch = "test"

# File name to edit
file_name = "main.tf"

# word to find
word_find = "weekend"

# work to replace
word_replace = "develop"

# Open the .txt file and clone the repositories
with open(input_file, "r") as file:
    for line in file:
        repo_url = line.strip()
        clone_repository(repo_url, target_branch)

print("Clone and Change on the file made with success")
