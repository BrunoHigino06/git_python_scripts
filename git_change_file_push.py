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
    repo = git.Repo.clone_from(repo_url, repo_dir, branch= branch_name)

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
        for line_number, line in enumerate(file, start=1):
            # Modify line 4 (index 3) of the main.tf file
            if line_number == file_line:
                print(line_change)
            else:
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

# Line of the file to edit
file_line = 4

# What needs to be added in the line of the file that will be edited
line_change = "change made using the script"

# Open the .txt file and clone the repositories
with open(input_file, "r") as file:
    for line in file:
        repo_url = line.strip()
        clone_repository(repo_url, target_branch)

print("Clone and Change on the file made with success")
