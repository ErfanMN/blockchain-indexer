# Blockchain Indexer

A high-performance blockchain indexing tool using DynamoDB and Node.js.

---
## Install all required packages:

```bash
npm install
```

## Setup Instructions

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
