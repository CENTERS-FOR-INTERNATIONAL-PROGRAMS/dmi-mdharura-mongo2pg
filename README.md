# mongodb-to-postgresql

A tool to migrate and replicate data from MongoDB to PostgreSQL in Node.js.

The tool works by creating an object data model which includes all data fields that you want to migrate to SQL and executing a migration script. The tool has the option to either run as a one off script, or can be setup to keep your replicated PostgreSQL database in sync with MongoDB.

## Getting Started

These instructions will get the project set up on your local machine for running the tool.

### Prerequisites

1. Node.js (Tested on V10.15.3)
2. Node Package Manager (Tested on 6.4.1)
3. PostgreSQL server (Tested on 10.3)
4. MongoDB server (Tested on 4.0.0)

### Setup

There are four steps for setup:

1. Setting up your local environment
2. Configure database connection settings
3. Run scripts

#### Environment Setup

Once you have cloned this repository into a local folder, navigate to the folder in your choice of console/terminal, and run:

```
npm install
```

Once installed, you can type 'npm list' into the command prompt and should see something similar to (list below is truncated and will include more packages in your local install):

```
+-- async@3.1.0
+-- async-waterfall@0.1.5
+-- moment@2.24.0
+-- dotenv@10.0.0
+-- mongodb@3.2.7
| +-- mongodb-core@3.2.7
`-- pg@7.11.0
  +-- buffer-writer@2.0.0
  +-- packet-reader@1.0.0
  +-- pg-connection-string@0.1.3
  +-- pg-pool@2.0.6
```

#### Database Connection Settings

create a .env file and enter your MongoDB and PostgreSQL database connection settings in the .env file:

```
Example:

MONGO_DB_NAME= 'my-dB'
MONGO_DB_URL = mongodb://localhost:27017

PG_DB_USER = 'postgres'
PG_DB_HOST = 'localhost'
PG_DB_NAME = 'my-db-analytics'
PG_DB_PASSWORD = 'xxxxxx'
PG_DB_PORT = '5432'


```

#### Run Database Creation & Migration Scripts

There are two basic scripts for this tool. To run them, navigate to the root folder of this repository and run:

```
node --trace-warnings start.js createdb
```

This will create the database model defined in the data_model.js file into your chosen Postgres instance. For basic changes, you can make changes to your data model and re-run the script to add or delete columns.

```
node --trace-warnings start.js migratedata
```

This will execute the data migration from MongoDB to PostgreSQL based on the data model and custom rules setup. This can be run once to migrate data as a one off process. Optionally, if using 'created_at' and 'updated_at' timestamp dates in MongoDB and these have been included in the data_model.js setup, the script can be run at regular intervals to transfer new or updated documents to Postgres
