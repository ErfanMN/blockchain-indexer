const AWS = require('aws-sdk');
const { promisify } = require('util');
const dotenv = require('dotenv');
const zlib = require('zlib');
const { log } = require('console');

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);
const base64Encode = (buffer) => buffer.toString('base64');
const base64Decode = (data) => Buffer.from(data, 'base64');
const MAX_BATCH_SIZE = 25;
const MAX_CONCURRENT_REQUESTS = 10; // Control the number of parallel requests to avoid throttling
const DYNAMODB_CHUNK_SIZE = 400000; // Example, adjust based on your needs
dotenv.config(); // Load environment variables from .env file


// DynamoDB Configuration
AWS.config.update({
    region: 'us-west-2', // Replace with your region
    endpoint: process.env.DYNAMODB_ENDPOINT // Use environment variable for endpoint
});

class DynamoDB {
    constructor(endpoint = process.env.DYNAMODB_ENDPOINT) {
        this.endpoint = endpoint;
        this.dynamodb = new AWS.DynamoDB();
        this.docClient = new AWS.DynamoDB.DocumentClient({
            maxRetries: 10,              // More retries for throttled writes
            httpOptions: {
              timeout: 5000,              // Shorter timeout for each request
              connectTimeout: 5000,
            }
          });
        this.transactions_table = "transactions";
        this.addrhistory_table = "addrhistory";
        this.txouts_table = "txouts";
        this.block_table = "block_metadata";
    }

    async _getLatestBlockMetadata() {
        const params = {
            TableName: this.block_table,
            Key: { id: "global_state" }
        };

        try {
            const data = await this.docClient.get(params).promise();
            return data.Item || {};
        } catch (error) {
            console.error("Error retrieving latest block metadata:", error);
            return {};
        }
    }

    async verifyAndBackup() {
        try {
            // Retrieve the latest block metadata
            const blockMetadata = await this._getLatestBlockMetadata();
            const blockcount = blockMetadata.blockcount;
            const timestamp = blockMetadata.timestamp;

            if (!blockcount || !timestamp) {
                console.error("Block metadata is missing blockcount or timestamp.");
                return;
            }

            // Perform verification logic (just logging here)
            console.log(`Verifying data for blockcount: ${blockcount}, timestamp: ${timestamp}`);

            // Backup the tables
            await this._backupTables(blockcount);
        } catch (error) {
            console.error("An unexpected error occurred during verification or backup:", error);
        }
    }

    async _backupTables(blockNumber) {
        const tablesToBackup = [this.block_table, this.transactions_table, this.addrhistory_table, this.txouts_table];

        for (const tableName of tablesToBackup) {
            const params = {
                TableName: tableName,
                BackupName: `${tableName}_backup_${blockNumber}`
            };

            try {
                const response = await this.dynamodb.createBackup(params).promise();
                if (response.BackupDetails && response.BackupDetails.BackupCreationDateTime) {
                    console.log(`Backup of table ${tableName} created successfully. Backup ARN: ${response.BackupDetails.BackupArn}`);
                } else {
                    console.error(`Failed to create backup for table ${tableName}`);
                }
            } catch (error) {
                console.error(`Error creating backup for table ${tableName}:`, error);
            }
        }
    }
    
    async _asyncBatchWrite(tableName, items) {
        const { default: pLimit } = await import('p-limit');

        const chunks = this._chunked(items, MAX_BATCH_SIZE);
        const limiter = pLimit(MAX_CONCURRENT_REQUESTS);
    
        const batchPromises = chunks.map(chunk => limiter(async () => {
            const batchItems = chunk.map(item => ({
                PutRequest: { Item: item }
            }));
    
            const params = {
                RequestItems: {
                    [tableName]: batchItems
                }
            };
    
            try {
                await this.docClient.batchWrite(params).promise();
            } catch (error) {
                console.error(`Error writing to ${tableName}:`, error);
            }
        }));
    
        // Wait for all limited batch writes to complete
        await Promise.all(batchPromises);
    }
    
    // Chunking function to divide items into smaller batches
    _chunked(items, size) {
        const result = [];
        for (let i = 0; i < items.length; i += size) {
            result.push(items.slice(i, i + size));
        }
        return result;
    }
    

    async _asyncBatchRead(tableName, keys) {
        const batchSize = 100; // Adjusted for your needs (DynamoDB max limit is 100 items)
        let results = [];

        for (let i = 0; i < keys.length; i += batchSize) {
            const batchKeys = keys.slice(i, i + batchSize);
            const params = {
                RequestItems: {
                    [tableName]: {
                        Keys: batchKeys,
                        ConsistentRead: true
                    }
                }
            };

            let retries = 5;
            let response;

            for (let attempt = 0; attempt < retries; attempt++) {
                try {
                    response = await this.dynamodb.batchGetItem(params).promise();
                    results = results.concat(response.Responses[tableName] || []);
                    const unprocessedKeys = response.UnprocessedKeys[tableName]?.Keys || [];

                    if (unprocessedKeys.length === 0) {
                        break;
                    }

                    // Retry with unprocessed keys
                    params.RequestItems[tableName].Keys = unprocessedKeys;
                    await sleep(0.01 * Math.pow(2, attempt)); // Exponential backoff
                } catch (error) {
                    console.error(`Error in batch read: ${error}`);
                    break;
                }
            }
        }

        return results;
    }

    async _compressData(data) {
        const jsonData = JSON.stringify(data);
        const compressedData = await gzip(jsonData);
        return base64Encode(compressedData);
    }

    async _decompressData(data) {
        const compressedData = base64Decode(data);
        const decompressedData = await gunzip(compressedData);
        return JSON.parse(decompressedData.toString());
    }

    _estimateItemSize(item) {
        let size = 0;
        for (const [key, value] of Object.entries(item)) {
            size += Buffer.byteLength(key, 'utf8');
            if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
                size += Buffer.byteLength(value.toString(), 'utf8');
            } else if (Buffer.isBuffer(value)) {
                size += value.length;
            } else if (Array.isArray(value) || typeof value === 'object') {
                size += Buffer.byteLength(JSON.stringify(value), 'utf8');
            } else {
                throw new Error(`Unsupported value type in item size estimation: ${typeof value}`);
            }
        }
        return size;
    }

    _splitLargeData(txid, fieldName, data) {
        const chunkSize = DYNAMODB_CHUNK_SIZE;
        const chunks = [];
        for (let i = 0; i < data.length; i += chunkSize) {
            chunks.push({
                txid,
                chunk_info: `${fieldName}_${i}`,
                [fieldName]: data.slice(i, i + chunkSize)
            });
        }
        return chunks;
    }

    _processLargeItems(txData) {
        const itemSize = this._estimateItemSize(txData);
        if (itemSize < DYNAMODB_CHUNK_SIZE) {
            txData.chunk_info = 'metadata_0'; // Ensure metadata chunk is labeled
            return [txData];
        }

        const txid = txData.txid;
        const baseTxData = { ...txData };
        delete baseTxData.vins;
        delete baseTxData.vouts;
        baseTxData.chunk_info = 'metadata_0';

        const splitItems = [baseTxData];
        splitItems.push(...this._splitLargeData(txid, 'vins', txData.vins));
        splitItems.push(...this._splitLargeData(txid, 'vouts', txData.vouts));

        console.log(`Transaction ${txid} split into ${splitItems.length} chunks.`);
        return splitItems;
    }

    async writeTxDetail(txid, txTime, txSize, vins, vouts) {
        const [compressedVins, compressedVouts] = await Promise.all([
            this._compressData(vins),
            this._compressData(vouts)
        ]);

        const txData = {
            txid,
            tx_time: txTime,
            tx_size: txSize,
            vins: compressedVins,
            vouts: compressedVouts
        };

        return this._processLargeItems(txData);
    }

    async writeAddrHistory(txid, txTime, vins, vouts, timeCounters) {
        const addrHistoryBatch = [];
        for (const item of [...vins, ...vouts]) {
            const addr = item.address;
            let count = timeCounters[`${addr}-${txTime}`] || 0;
            const adjustedTime = txTime + count;
            timeCounters[`${addr}-${txTime}`] = count + 1;

            addrHistoryBatch.push({
                addr,
                time: adjustedTime,
                txid
            });
        }
        return addrHistoryBatch;
    }

    async storeTransactions(block_data) {
        const txs = block_data.tx;
        const txTime = parseInt(block_data.time, 10);
        const startTotal = Date.now();
    
        // Start timing for fetching outputs and inputs
        let startOutputsInputs = performance.now();
        const formattedVouts = await Promise.all(txs.map(tx => this.writeOutputs(tx.txid, tx.vout)));
        const formattedVins = await Promise.all(txs.map(tx => this.readInputs(tx.txid, tx.vin)));
        let endOutputsInputs = performance.now();
        let outputsInputsTime = (endOutputsInputs - startOutputsInputs) / 1000; // in seconds
    
        const txDetailBatchPromises = [];
        const addrHistoryBatchPromises = [];
        const timeCounters = {};
        const deleteTasks = [];
    
        // Start timing for processing each transaction
        let startProcessing = performance.now();
        for (let idx = 0; idx < txs.length; idx++) {
            const tx = txs[idx];
            const txid = tx.txid;
            const txSize = parseInt(tx.size, 10);
            txDetailBatchPromises.push(this.writeTxDetail(txid, txSize, txTime, formattedVins[idx], formattedVouts[idx]));
            addrHistoryBatchPromises.push(this.writeAddrHistory(txid, txTime, formattedVins[idx], formattedVouts[idx], timeCounters));
            deleteTasks.push(this.deletePrevVouts(formattedVins[idx]));
        }
        let endProcessing = performance.now();
        let processingTime = (endProcessing - startProcessing) / 1000; // in seconds
    
        // Start timing for batch write
        let startBatchWrite = performance.now();
        const [txDetailBatchArrays, addrHistoryBatchArrays] = await Promise.all([
            Promise.all(txDetailBatchPromises),
            Promise.all(addrHistoryBatchPromises),
        ]);
    
        // Flatten the arrays
        const txDetailBatch = txDetailBatchArrays.flat();
        const addrHistoryBatch = addrHistoryBatchArrays.flat();
    
        // Time the actual batch write
        await Promise.all([
            this._asyncBatchWrite(this.transactions_table, txDetailBatch),
            this._asyncBatchWrite(this.addrhistory_table, addrHistoryBatch),
            ...deleteTasks
        ]);
        let endBatchWrite = performance.now();
        let batchWriteTime = (endBatchWrite - startBatchWrite) / 1000; // in seconds
   
        const totalTime = (Date.now() - startTotal) / 1000;
        console.log(`storeTransactions Timing: Total=${totalTime.toFixed(3)}s | Fetch Outputs/Inputs=${outputsInputsTime.toFixed(3)}s | Process Transactions=${processingTime.toFixed(3)}s | Batch Write=${batchWriteTime.toFixed(3)}s`);
    }
    
    async writeOutputs(txid, outputs) {
        if (!outputs || outputs.length === 0) {
            return [];
        }
    
        const formattedVouts = outputs
            .filter(output => output.scriptPubKey)
            .map(output => ({
                txid: String(txid),
                vout_index: output.n,
                address: (output.scriptPubKey.addresses && output.scriptPubKey.addresses.length > 0)
                    ? output.scriptPubKey.addresses[0]
                    : 'NO_ADDRESS',
                value: Math.floor(output.value * 1e8),
            }));
    
        // If there are any outputs to write, pass the formattedVouts to _asyncBatchWrite
        if (formattedVouts.length > 0) {
            await this._asyncBatchWrite(this.txouts_table, formattedVouts);  // _asyncBatchWrite will wrap items in PutRequest
        }
    
        return formattedVouts;
    }

    async deletePrevVouts(vins) {
        const batchWriteParams = vins.map(vin => ({
            DeleteRequest: {
                Key: {
                    txid: vin.txid,
                    vout_index: vin.vout_index
                }
            }
        }));

        if (batchWriteParams.length > 0) {
            const params = { RequestItems: { [this.txouts_table]: batchWriteParams } };
            await this.dynamodb.batchWriteItem(params).promise();
        }
    }

    async readInputs(txid, inputs) {
        // Ensure the keys are properly formatted for batchGet
        const keys = inputs.map(input => {
            if (input.txid && input.vout_index !== undefined && input.vout_index !== null) {
                return {
                    txid: String(input.txid),        // Partition key (must be string)
                    vout_index: Number(input.vout_index)     // Sort key (must be number)
                };
            }
            return null; // Explicitly return null if invalid
        }).filter(Boolean); // Filter out invalid keys

        // If no keys are provided, return an empty array
        if (keys.length === 0) return [];
        const params = {
            RequestItems: {
                [this.txouts_table]: {
                    Keys: keys 
                }
            }
        };

        try {
            // Perform the batch get request
            const response = await this.docClient.batchGet(params).promise();
            let results = response.Responses.txouts || [];
    
            // Handle cases where not all results are returned
            if (results.length !== keys.length) {
                // Fallback to query if some results are missing
                const txData = await this.docClient.query({
                    TableName: "txouts",
                    KeyConditionExpression: 'txid = :txid',
                    ExpressionAttributeValues: { ':txid': txid }
                }).promise();
    
                // Process missing inputs by decompressing data
                const vinsChunks = txData.Items.filter(item => item.chunk_info.startsWith('vins_'))
                    .sort((a, b) => parseInt(a.chunk_info.split('_')[1]) - parseInt(b.chunk_info.split('_')[1]))
                    .map(item => item.vins)
                    .join('');
    
                if (vinsChunks) {
                    results = await this._decompressData(vinsChunks);
                }
            }
    
            // Convert values to proper types for consistency
            results.forEach(item => {
                item.vout_index = parseInt(item.vout_index, 10);
                item.value = parseInt(item.value, 10);
            });
    
            return results;
    
        } catch (error) {
            console.error("Error fetching inputs:", error);
            return [];
        }
    }
    
    async getBalance(addr) {
        const params = {
          TableName: this.txoutsTable,
          IndexName: "addr_index",
          KeyConditionExpression: "address = :address",
          ExpressionAttributeValues: {
            ":address": addr,
          },
        };
    
        const result = await dynamodb.query(params).promise();
        return result.Items.reduce((balance, txout) => balance + txout.value, 0);
      }
    
      async lookupTxDetail(txid) {
        const params = {
          TableName: this.transactionsTable,
          KeyConditionExpression: "txid = :txid",
          ExpressionAttributeValues: {
            ":txid": txid,
          },
        };
    
        const result = await dynamodb.query(params).promise();
        const items = result.Items || [];
    
        if (!items.length) return null;
    
        let baseTxItem = null;
        let vinsChunks = [];
        let voutsChunks = [];
    
        items.forEach((item) => {
          const chunkInfo = item.chunk_info;
          if (chunkInfo === "metadata_0") {
            baseTxItem = item;
          } else if (chunkInfo.startsWith("vins_")) {
            vinsChunks.push({ index: parseInt(chunkInfo.split("_")[1]), vins: item.vins });
          } else if (chunkInfo.startsWith("vouts_")) {
            voutsChunks.push({ index: parseInt(chunkInfo.split("_")[1]), vouts: item.vouts });
          }
        });
    
        if (!baseTxItem) return null;
    
        vinsChunks.sort((a, b) => a.index - b.index);
        voutsChunks.sort((a, b) => a.index - b.index);
    
        const vinsData = vinsChunks.map((chunk) => chunk.vins).join("");
        const voutsData = voutsChunks.map((chunk) => chunk.vouts).join("");
    
        return {
          txid: baseTxItem.txid,
          time: baseTxItem.tx_time,
          size: baseTxItem.tx_size,
          vins: await this.decompressData(vinsData),
          vouts: await this.decompressData(voutsData),
        };
      }
    
      async updateBlockMetadata(blockcount, timestamp) {
        const params = {
          TableName: "block_metadata",
          Item: {
            id: "global_state",
            blockcount,
            timestamp,
          },
        };
    
        await this.docClient.put(params).promise();
      }
    
    async getLatestBlockcount() {
        const params = {
            TableName: "block_metadata", // Use this.block_table to reference the block table
            Key: { id: "global_state" },
        };
    
        // Replace dynamodb with this.dynamodb
        const result = await this.docClient.get(params).promise();
        return result.Item ? result.Item.blockcount : 0;
    }
    
}

module.exports = DynamoDB;
