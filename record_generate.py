# DNS Records to Terraform Blocks Generator
#
# This Python script is designed to read a list of domain names from a 'domain.txt' file, execute the 'nslookup' command for each domain,
# and then create Terraform configuration blocks based on the obtained DNS information. The generated Terraform blocks are intended for use with the AWS Route 53 service.
#
# The script performs the following steps:
# 1. Reads a list of domain names from 'domain.txt' file, where each line contains a single domain name.
# 2. For each domain, it executes the 'nslookup' command to retrieve DNS information.
# 3. Parses the 'nslookup' output to extract the Canonical Name (CNAME) value associated with the domain.
# 4. Constructs a Terraform block using the following format:
#    {
#        zone_id = module.hosted_zone.zone_ids[1]
#        name    = "domain name from 'domain.txt'"
#        type    = "CNAME"
#        ttl     = 300
#        records = ["CNAME value from 'nslookup' output"]
#    }
# 5. Appends these Terraform blocks to an output list.
# 6. Writes the generated Terraform blocks in JSON format to an 'output.tf' file.
#
# Prerequisites:
# - Ensure that 'domain.txt' contains a list of domain names you want to analyze.
# - The 'nslookup' command must be available on your system for DNS resolution.
#
# After running this script, you will have an 'output.tf' file containing Terraform blocks that can be used to configure DNS records for the specified domains in an AWS Route 53 hosted zone.
#
# Author: [Bruno Higino de Souza]
# Date: [25/10/2023]

import subprocess
import json

# Read the domains from the domain.txt file
with open('domain.txt', 'r') as domain_file:
    domains = domain_file.read().splitlines()

# List to store the generated blocks
output_blocks = []

# Loop through each domain
for domain in domains:
    # Execute nslookup to get domain information
    result = subprocess.run(["nslookup", domain], capture_output=True, text=True)

    # Parse the nslookup result to extract the value for the "name" field
    name_value = ""
    for line in result.stdout.splitlines():
        if "Nome:" in line:  # Procure por uma linha que contenha "Nome:" (pode variar com base no sistema)
            name_value = line.split("Nome:")[1].strip()  # Pode ser necessário ajustar a lógica com base na saída do seu sistema

    # Create the JSON block with the extracted "name" value and format records as a list with double quotes
    block = {
        "zone_id =":f'module.hosted_zone.zone_ids[1]"',
        "name =":f'"{domain}"',
        "type =":'"CNAME"',
        "ttl =":300,
        "records =": f'["{name_value}"]'  # Use the original domain as the value of CNAME
    }

    # Add the block to the output list
    output_blocks.append(block)

# Write the generated blocks to the output.tf file with the specified format
with open('output.tf', 'w') as output_file:
    for custom_block in output_blocks:
        output_file.write("{\n")
        for key, value in custom_block.items():
            output_file.write(f'    {key}: {value},\n')
        output_file.write("},\n")

print("Blocks generated and written to output.tf")

