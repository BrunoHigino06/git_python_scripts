import os
import git

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

        # Remove main.tf and var.tf if they exist
        remove_files(repo_dir, files_to_remove)

        # Add, commit, and push the changes
        add_commit_push(repo)

    else:
        print(f"The branch '{branch_name}' does not exist in the repository '{repo_name}'.")


# Function to remove specific files from a directory
def remove_files(directory, filenames):
    for filename in filenames:
        file_path = os.path.join(directory, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Removed '{filename}' from '{directory}'.")


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
input_file = "remove_push.txt"

# Name of the branch to which you want to clone the repositories
target_branch = "test"

# Files name to remove
files_to_remove = ["output.tf", "vars.tf"]

# Open the .txt file and clone the repositories
with open(input_file, "r") as file:
    for line in file:
        repo_url = line.strip()
        clone_repository(repo_url, target_branch)

print("Clone and Change on the file made with success")