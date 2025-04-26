# Blockchain Indexer

A high-performance blockchain indexing tool using DynamoDB and Node.js.

---
## Install all required packages:

```bash
npm install
```

## Setup Instructions

### setup env variables
Create a .env file in the project root and add the following settings:

```bash
DYNAMODB_ENDPOINT=http://localhost:8000
BITCOIND_ENDPOINT=http://<user>:<pass>@localhost:48332
FETCH_BLK_BATCH=100
INDEXING_ATTEMP_INTERVAL=10
BLOCK_BACKUP_INTERVAL=100000
```
### Create and Delete DynamoDB Tables

Use the following commands:

- To **delete** existing DynamoDB tables:
  ```bash
  node utils.js delete
  ```

- To **create** new DynamoDB tables:
  ```bash
  node utils.js create
  ```

---

## Running the Indexer

Start the project with:

```bash
node index.js
```

---

## Notes

- Make sure your AWS credentials are properly configured before running the project.
- Install dependencies first using:
  ```bash
  npm install
  ```
