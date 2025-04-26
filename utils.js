const AWS = require("aws-sdk");
const dotenv = require("dotenv");
const { promisify } = require("util");

dotenv.config();

// Configure AWS DynamoDB
AWS.config.update({
    region: "us-west-2", // Change to your AWS region
    endpoint: process.env.DYNAMODB_ENDPOINT || "http://localhost:8000" // Default to local DynamoDB endpoint if not provided
});

const dynamoDB = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

// Wrapper for async DynamoDB operations
const createTable = promisify(dynamoDB.createTable.bind(dynamoDB));
const listTables = promisify(dynamoDB.listTables.bind(dynamoDB));

// Define the classes and methods to handle DynamoDB tables
class IndexingDb {
    constructor() {
        this.dynamoDB = dynamoDB;
    }

    async createBlockMetadata() {
        const params = {
            TableName: "block_metadata",
            KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
            AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
            BillingMode: "PAY_PER_REQUEST"
        };
        try {
            const result = await createTable(params);
            console.log("Table block_metadata created successfully", result);
        } catch (err) {
            console.error("Error creating table block_metadata:", err);
        }
    }

    async createTxouts() {
        const params = {
            TableName: "txouts",
            KeySchema: [
                { AttributeName: "txid", KeyType: "HASH" },
                { AttributeName: "vout_index", KeyType: "RANGE" }
            ],
            AttributeDefinitions: [
                { AttributeName: "txid", AttributeType: "S" },
                { AttributeName: "vout_index", AttributeType: "N" },
                { AttributeName: "address", AttributeType: "S" }
            ],
            BillingMode: "PAY_PER_REQUEST",
            GlobalSecondaryIndexes: [
                {
                    IndexName: "addr_index",
                    KeySchema: [{ AttributeName: "address", KeyType: "HASH" }],
                    Projection: { ProjectionType: "ALL" }
                }
            ]
        };
        try {
            const result = await createTable(params);
            console.log("Table txouts created successfully", result);
        } catch (err) {
            console.error("Error creating table txouts:", err);
        }
    }

    async createAddrhistory() {
        const params = {
            TableName: "addrhistory",
            KeySchema: [
                { AttributeName: "addr", KeyType: "HASH" },
                { AttributeName: "time", KeyType: "RANGE" }
            ],
            AttributeDefinitions: [
                { AttributeName: "addr", AttributeType: "S" },
                { AttributeName: "time", AttributeType: "N" }
            ],
            BillingMode: "PAY_PER_REQUEST"
        };
        try {
            const result = await createTable(params);
            console.log("Table addrhistory created successfully", result);
        } catch (err) {
            console.error("Error creating table addrhistory:", err);
        }
    }

    async createTransactions() {
        const params = {
            TableName: "transactions",
            KeySchema: [
                { AttributeName: "txid", KeyType: "HASH" },
                { AttributeName: "chunk_info", KeyType: "RANGE" }
            ],
            AttributeDefinitions: [
                { AttributeName: "txid", AttributeType: "S" },
                { AttributeName: "chunk_info", AttributeType: "S" }
            ],
            BillingMode: "PAY_PER_REQUEST"
        };
        try {
            const result = await createTable(params);
            console.log("Table transactions created successfully", result);
        } catch (err) {
            console.error("Error creating table transactions:", err);
        }
    }

    async createTables() {
        try {
            const existingTables = await listTables();
            console.log("Existing Tables Before Creation:", existingTables.TableNames);

            const tables = existingTables.TableNames;

            if (!tables.includes("transactions")) await this.createTransactions();
            if (!tables.includes("addrhistory")) await this.createAddrhistory();
            if (!tables.includes("txouts")) await this.createTxouts();
            if (!tables.includes("block_metadata")) await this.createBlockMetadata();

            // List the tables again to verify
            const newTables = await listTables();
            console.log("Existing Tables After Creation:", newTables.TableNames);
        } catch (err) {
            console.error("Error during table creation:", err);
        }
    }
    async deleteTable(tableName) {
        const params = {
            TableName: tableName
        };
        try {
            const result = await this.dynamoDB.deleteTable(params).promise();
            console.log(`Table ${tableName} deleted successfully`, result);
        } catch (err) {
            console.error(`Error deleting table ${tableName}:`, err);
        }
    }

    async deleteTables() {
        try {
            const existingTables = await listTables();
            console.log("Existing Tables Before Deletion:", existingTables.TableNames);

            const tables = existingTables.TableNames;

            if (tables.includes("transactions")) await this.deleteTable("transactions");
            if (tables.includes("addrhistory")) await this.deleteTable("addrhistory");
            if (tables.includes("txouts")) await this.deleteTable("txouts");
            if (tables.includes("block_metadata")) await this.deleteTable("block_metadata");

            // List the tables again to verify deletion
            const newTables = await listTables();
            console.log("Existing Tables After Deletion:", newTables.TableNames);
        } catch (err) {
            console.error("Error during table deletion:", err);
        }
    }

}

// Main function to handle commands
async function main() {
    const dbobj = new IndexingDb();
    const command = process.argv[2];

    if (command === "create") {
        await dbobj.createTables();
    } else if (command === "delete") {
        await dbobj.deleteTables();
    } else if (command === "backup") {
        await dbobj.backupTables();
    } else {
        console.log("Usage: node utils.js create/delete/backup");
    }
}

main().catch(console.error);
