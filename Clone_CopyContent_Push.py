import os
import shutil
import git

# Path to the repositories.txt file
repositories_file = 'repositories.txt'

# Branch of the repository
branch = 'feature/weekend'

# Paths to the vpc_endpoint.tf and terraform.tfvars files
vpc_endpoint_file = 'vpc_endpoint.tf'
terraform_tfvars_file = 'terraform.tfvars'

# Working directory where repositories will be cloned
working_directory = 'temp_repositories'

# Read the list of repositories from the repositories.txt file
with open(repositories_file, 'r') as file:
    repositories = file.readlines()

# Remove newline characters
repositories = [repo.strip() for repo in repositories]

# Loop through the repositories
for repository_url in repositories:
    # Clone the repository from the feature/weekend branch
    repo_name = repository_url.split('/')[-1]
    repo_dir = os.path.join(working_directory, repo_name)

    if os.path.exists(repo_dir):
        print(f"Repository '{repo_name}' has already been cloned. Skipping to the next.")
        continue

    print(f"Cloning repository '{repo_name}'...")
    git.Repo.clone_from(repository_url, repo_dir, branch=branch)

    # Copy the content of vpc_endpoint.tf to the cloned repository
    vpc_endpoint_source = os.path.join(os.getcwd(), vpc_endpoint_file)
    vpc_endpoint_destination = os.path.join(repo_dir, vpc_endpoint_file)

    shutil.copy(vpc_endpoint_source, vpc_endpoint_destination)

    # Copy the content of terraform.tfvars to the cloned repository
    terraform_tfvars_source = os.path.join(os.getcwd(), terraform_tfvars_file)
    terraform_tfvars_destination = os.path.join(repo_dir, 'terraform.tfvars')

    shutil.copy(terraform_tfvars_source, terraform_tfvars_destination)

    # Commit and push the changes
    print(f"Committing and pushing changes to repository '{repo_name}'...")
    repo = git.Repo(repo_dir)
    repo.index.add([vpc_endpoint_file, 'terraform.tfvars'])
    repo.index.commit("Automated update of files")
    origin = repo.remote('origin')
    origin.push()

print("Process completed.")