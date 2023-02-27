# Fetch Data Engineering Take Home: ETL off a SQS Queue 


## pii-masking script


The pii-masking script reads user login information from an SQS queue, encrypts the personal data in it and persists the data in a postgres database. 

The encryption/masking is done such that the data can be decrypted and duplicate values can be identified.


## Requirements and Installations


1. docker: [Installation Steps](https://docs.docker.com/get-docker/)
2. python: [Installation Steps](https://docs.python-guide.org/starting/install3/osx/)
3. localstack
    `pip install localstack`
4. aws-local
    `pip install awscli`
5. pandas
    `pip install pandas`
6. psycopg2
    `pip install psycopg2-binary`
7. cryptography
    `pip install cryptography`
8. Homebrew:
    `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
9. Postgres:
    `brew install postgresql@14`
10. boto3:
    `pip install boto3`


## Usage


1. Having the directory containing pii-masking.py and docker-compose.yml as the working directory in the terminal run: 
    `docker-compose up -d`
2. Run the python script using the command
    `python pii-masking.py`
3. To bring down docker, run:
    `docker-compose down --remove-orphans`
4. To clean docker build files, run:
    `docker system prune -f`


## Next Steps


1. The SQS queue, server, and postgres configs can be moved to a dedicated config file away from the core logic.
2. For encryption, I have initialized a new private key every time the script is run. But, this key must be generated only once and stored somewhere safe for later decryption. We must use the same key everytime.
3. DB insert, select, and truncate queries must ideally sit in dedicated SQL files outside of the core logic.
4. Due to lack of time, in the application, I read the data from the queue completely, and then proceed towards data encryption using a private key, and finally, persist the masked data in the db. If I had more time, I would use an in-memory buffer to act as an interface between the SQS queue consumer and postgres db. The consumer and db writer should run on dedicated threads or processes to isolate/containerize them. The current implementation: reading everything, then processing, then writing is not ideal for queue based or event-driven based systems. A fault or break in either the queue consumer or db writer can break the whole application, but the damage can be contained if the cosnumer and writer were isolated.

Please read [Data-Engineer-Intern - Questions.txt](https://github.com/hafeezali/Fetch-Data-Engineer-Intern/blob/main/Data-Engineer-Intern%20-%20Questions.txt) for more elaborate next steps towards deploying this application to production
