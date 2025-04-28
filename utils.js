const { 
    DynamoDBClient, 
    CreateTableCommand, 
    ListTablesCommand, 
    DeleteTableCommand 
} = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
const dotenv = require("dotenv");
dotenv.config();
docClient = new DynamoDBClient({
    region: process.env.AWS_REGION,
    endpoint: process.env.DYNAMODB_ENDPOINT,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
});
dynamoDB = DynamoDBDocumentClient.from(docClient);
const Redis = require('ioredis');
// Wrapper for async DynamoDB operations

async function createTable(params) {
    const command = new CreateTableCommand(params);
    return await docClient.send(command);
}

async function listTables() {
    const command = new ListTablesCommand({});
    return await docClient.send(command);
}

async function deleteTable(tableName) {
    const command = new DeleteTableCommand({ TableName: tableName });
    return await docClient.send(command);
}

// Define the classes and methods to handle DynamoDB tables
class IndexingDb {
    constructor() {
        this.dynamoDB = dynamoDB;
        this.redis = new Redis(process.env.REDIS_CONNECTION_STRING);
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
            const result = await deleteTable(params);
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

            if (tables.includes("transactions")) await deleteTable("transactions");
            if (tables.includes("addrhistory")) await deleteTable("addrhistory");
            if (tables.includes("block_metadata")) await deleteTable("block_metadata");
            await this.redis.flushdb();
            await this.redis.quit();
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
    process.exit(0);
}

main().catch(console.error);
