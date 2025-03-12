# seng3011-sierra-data-collection
Data Collection script. Utilising AWS lambda functions to GET, store and convert ESG data.


# Sierra Data Collection Microservice

## Overview 
This microservice takes raw *Environmental, Social, and Governance* data and uploads it through AWS lambda into an s3 bucket. 
The data is then converted to `PARQUET` format, making it easily queriable. 

## Features 

- Trigerrable Data collection 
- AWS lambda deployment 
- Querable data 
- Linting and Code Quality checks

## Technology Stack 

| Element | Purpose | Notes | 
| ------- | ------- | ----- |
| python | Main development language | currently running 3.1 |
| AWS Lambda | Serverless hosting | currently in devs |
| Github Actions | ci/cd | base deployment comlete more to be added |
| Flake8 & Black | linting | | 
| s3 | Database storage | In devs |

## Licence 

CC BY-NC-SA
 main
