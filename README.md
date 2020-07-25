# Sparkify Analytics Database

## Introduction
This project involved designing and creating an analytics database in the cloud for a hypothetical start up called Sparkify. 
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
To this end, a simple python based ETL pipeline was developed to extract data from local sources and load it into a star 
schema design. The choice to use a star schema was based on its high query performance relative to non-dimensional 
databases, its ease of use for end users and its ease of use with business intelligence tools.
## Requirements
Instructions for how to run assumes the following are installed:
* python 3.6+ and pip
* a package for managing virtual environments (e.g [venv](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/)) 
* an AWS account
* configured AWS credentials, as described in [Quickstart](https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/quickstart.html).
        
Prior to running the code the following can be done on the command line:
* Navigate to folder where the repository will be stored
    ```commandline
    cd ~/path_to_folder/
    ``` 
* Clone the repository
    ```commandline
    git clone git@github.com:paulb17/sparkify_cloud_etl.git
    ``` 
* Create and activate a virtual environment
    ```commandline
     python3 -m venv venv
     source venv/bin/activate
    ```
* Install the needed packages 
    ```commandline
    pip install -r requirements.txt
    ```
  
## Running the ETL pipeline
For an initial run, use the following command:
```commandline
   python3 etl.py 
```

By default the above command will setup the needed AWS Services (Redshift, IAM role and security group), create all 
tables in the database (dropping any existing table), copy the data from S3 to staging tables on Redshift, and utilize
    SQL queries to transform and load the data into the database tables. To simply drop any pre-existing table 
in the Sparkify database and recreate empty ones, run the following command can be run:

```commandline
   python3 create_tables.py
```

To delete the created redshift cluster and IAM role policy run the command:
```commandline
   python3 create_tables.py -d
```

## Some improvements that could be made:
* Inclusion of surrogate keys in the design
* Addition of unit tests
* Better software architecture for the code
