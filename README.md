
# DynamicSQLDataExtractor

**DynamicSQLDataExtractor** is a Python-based tool designed to streamline the execution of dynamic SQL queries against MySQL databases. Leveraging AWS Secrets Manager for secure credential management, this tool enables users to securely access and query databases without exposing sensitive information. Through an intuitive command-line interface, users can select predefined queries and specify date ranges for data extraction. The extracted data is then efficiently processed and saved as CSV files in a designated directory, making it easy to generate custom reports based on the latest data.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Configuration](#configuration)
- [Usage](#usage)
- [Contact](#contact)

## Features

- Securely manage database credentials using AWS Secrets Manager.
- Execute dynamic SQL queries against MySQL databases.
- Select queries and specify date ranges via a command-line interface.
- Process and save extracted data as CSV files for easy report generation.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python 3.x installed
- AWS CLI configured with appropriate permissions
- MySQL database accessible

### Installation

Clone the repository and install dependencies:
 - git clone https://github.com/yourusername/DynamicSQLDataExtractor.git 
 - cd DynamicSQLDataExtractor pip install -r requirements.txt


### Configuration

1. Store your MySQL credentials in AWS Secrets Manager.
2. Create a `queries.json` file with your SQL queries and descriptions.

## Usage

Run the tool from the command line:
-  python main.py --aws-profile your_aws_profile --secret-name your_secret_name


Follow the prompts to select a query and enter date ranges.

## Contact

My email- arun.puram@outlook.com

