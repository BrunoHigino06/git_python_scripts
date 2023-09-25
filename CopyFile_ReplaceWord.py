import os

# Path to the AWS accounts file
aws_accounts_path = 'aws_accounts.txt'

# Source file path
source_path = "C:\\Users\\Bruno\\Desktop\\clients\\transbank\\hosted_zones\\infra\\terraform\\pci-c2-prod-common.tf"

# Read the AWS accounts file
with open(aws_accounts_path, 'r') as aws_file:
    aws_accounts = aws_file.read().splitlines()

# Loop to create files for each AWS account
for aws_account in aws_accounts:
    # Split the string into account_name and account_zone using a comma as the separator
    account_name, account_zone = aws_account.split(',')

    # Define the name of the new file
    new_file = f'{account_name}.tf'

    # Copy the content from the source file to the new file
    with open(source_path, 'r') as source_file, open(new_file, 'w') as destination_file:
        content = source_file.read()
        # Perform text replacement
        replaced_content = content.replace('pci-c2-prod-common', account_name).replace('pcic2-p-comm.prod', account_zone)
        destination_file.write(replaced_content)

    print(f'File {new_file} created successfully, and text replaced!')

print('All files have been created, and text has been replaced.')
