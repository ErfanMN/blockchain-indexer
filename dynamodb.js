import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import Redis from 'ioredis';
import { 
    DynamoDBDocumentClient,
    BatchWriteCommand
} from '@aws-sdk/lib-dynamodb';
import { promisify } from 'util';
import dotenv from 'dotenv';
import zlib from 'zlib';
import pLimit from 'p-limit';

// Initialize environment variables
dotenv.config();

// Promisify zlib methods
const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

// Base64 encoding and decoding
const base64Encode = (buffer) => buffer.toString('base64');
const base64Decode = (data) => Buffer.from(data, 'base64');

// Constants
const MAX_BATCH_SIZE = 25;
const MAX_CONCURRENT_REQUESTS = 10;
const DYNAMODB_CHUNK_SIZE = 400000;

// Set up concurrency limiter
const limiter = pLimit(MAX_CONCURRENT_REQUESTS);

export {
  DynamoDBClient,
  Redis,
  DynamoDBDocumentClient,
  BatchWriteCommand,
  gzip,
  gunzip,
  base64Encode,
  base64Decode,
  MAX_BATCH_SIZE,
  MAX_CONCURRENT_REQUESTS,
  DYNAMODB_CHUNK_SIZE,
  limiter
};


class DynamoDB {
    constructor(endpoint = process.env.DYNAMODB_ENDPOINT) {
        this.endpoint = endpoint;
        this.docClient = new DynamoDBClient({
            region: process.env.AWS_REGION,
            endpoint: process.env.DYNAMODB_ENDPOINT,
            credentials: {
                accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
            },
        });
        this.dynamodb = DynamoDBDocumentClient.from(this.docClient);
        this.redisClient = new Redis(process.env.REDIS_CONNECTION_STRING);
        this.transactions_table = "transactions";
        this.addrhistory_table = "addrhistory";
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
        const tablesToBackup = [this.block_table, this.transactions_table, this.addrhistory_table];

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
        if (!items || items.length === 0) return;
    
        const chunks = this._chunked(items, MAX_BATCH_SIZE);
    
        const batchPromises = chunks.map(chunk => limiter(() => this._writeBatchWithRetry(tableName, chunk)));
        await Promise.all(batchPromises);
    }
    
    // New helper function for retry logic
    async _writeBatchWithRetry(tableName, chunk, attempt = 0) {
        const MAX_RETRIES = 5;
    
        const batchItems = chunk.map(item => ({
            PutRequest: { Item: item }
        }));
    
        const params = {
            RequestItems: {
                [tableName]: batchItems
            }
        };
    
        try {
            const command = new BatchWriteCommand(params);
            const response = await this.docClient.send(command);
    
            const unprocessed = response.UnprocessedItems?.[tableName] || [];
            if (unprocessed.length > 0 && attempt < MAX_RETRIES) {
                console.warn(`Retrying ${unprocessed.length} unprocessed items (attempt ${attempt + 1})`);
                await this._writeBatchWithRetry(tableName, unprocessed.map(req => req.PutRequest.Item), attempt + 1);
            }
        } catch (err) {
            console.error(`Batch write failed for table ${tableName} (attempt ${attempt + 1}):`, err);
        }
    }
    
    // Chunking function to divide items into smaller batches
    _chunked(items, size) {
        const result = [];
        for (let i = 0; i < items.length; i += size) {
            result.push(items.slice(i, i + size));
        }
        return result;
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
            if (addr !== "NaN") {
                let count = timeCounters[`${addr}-${txTime}`] || 0;
                const adjustedTime = txTime + count;
                timeCounters[`${addr}-${txTime}`] = count + 1;

                addrHistoryBatch.push({
                    addr,
                    time: adjustedTime,
                    txid
                });
            }
        }
        return addrHistoryBatch;
    }

    async storeTransactions(block_data, tx_time) {
        const txs = block_data.tx;
        const startTotal = Date.now();
    
        // Step 1: Fetch outputs and inputs
        let startOutputsInputs = performance.now();
        const formattedVouts = await Promise.all(txs.map(tx => this.writeOutputs(tx.txid, tx.vout)));
        const formattedVins = await Promise.all(txs.map(tx => this.readInputs(tx.txid, tx.vin)));
        let endOutputsInputs = performance.now();
        let outputsInputsTime = (endOutputsInputs - startOutputsInputs) / 1000;
    
        // Step 2: Format data into batches
        let startProcessing = performance.now();
        const txDetailBatch = [];
        const addrHistoryBatch = [];
        const deleteTasks = [];
        const timeCounters = {};
    
        for (let idx = 0; idx < txs.length; idx++) {
            const tx = txs[idx];
            const txid = tx.txid;
            const txSize = parseInt(tx.size, 10);
    
            const txDetailItems = await this.writeTxDetail(txid, txSize, tx_time, formattedVins[idx], formattedVouts[idx]);
            const addrHistoryItems = await this.writeAddrHistory(txid, tx_time, formattedVins[idx], formattedVouts[idx], timeCounters);
            const deleteTask = this.deletePrevVouts(formattedVins[idx]);
    
            txDetailBatch.push(...txDetailItems);
            addrHistoryBatch.push(...addrHistoryItems);
            deleteTasks.push(deleteTask);
        }
        let endProcessing = performance.now();
        let processingTime = (endProcessing - startProcessing) / 1000;
    
        // Step 3: Batch write all at once
        let startBatchWrite = performance.now();
        await Promise.all([
            this._asyncBatchWrite(this.transactions_table, txDetailBatch),
            this._asyncBatchWrite(this.addrhistory_table, addrHistoryBatch),
            ...deleteTasks,
        ]);
        let endBatchWrite = performance.now();
        let batchWriteTime = (endBatchWrite - startBatchWrite) / 1000;
    
        const totalTime = (Date.now() - startTotal) / 1000;
        console.log(`storeTransactions Timing: Total=${totalTime.toFixed(3)}s | Fetch Outputs/Inputs=${outputsInputsTime.toFixed(3)}s | Process Transactions=${processingTime.toFixed(3)}s | Batch Write=${batchWriteTime.toFixed(3)}s`);
    }
    
    
    async writeOutputs(txid, outputs) {
        if (!outputs || outputs.length === 0) {
            return [];
        }
    
        const pipeline = this.redisClient.pipeline(); // Batch commands
        const formattedVouts = [];
    
        for (const output of outputs) {
            if (!output.scriptPubKey) continue;
            
            const address = output.scriptPubKey.address || 'NaN';
    
            const item = {
                address,
                value: Math.floor(output.value * 1e8),
            };
    
            const key = `${txid}:${output.n}`;
            pipeline.set(key, JSON.stringify(item));
    
            formattedVouts.push({
                txid: String(txid),
                vout_index: output.n,
                address: address,
                value: item.value,
            });
        }
    
        if (formattedVouts.length > 0) {
            await pipeline.exec();
        }
    
        return formattedVouts;
    }
    
    async deletePrevVouts(vins) {
        if (!vins || vins.length === 0) return;
        const keys = vins.map(vin => `${vin.txid}:${vin.vout_index}`);
        const pipeline = this.redisClient.pipeline();
        keys.forEach(key => pipeline.del(key));
        await pipeline.exec();
    }

    async readInputs(txid, inputs) {
        const keys = inputs.map(input => {
            if (input.txid && input.vout) {
                return `${input.txid}:${input.vout}`;
            }
            return null;
        }).filter(Boolean);
        if (keys.length === 0) return [];
    
        try {
            const results = await this.redisClient.mget(keys);
            const parsedResults = results.map((result, idx) => {
                if (result) {
                    const item = JSON.parse(result);
                    return {
                        txid: inputs[idx].txid,
                        vout_index: inputs[idx].vout,
                        address: item.address,
                        value: item.value,
                    };
                }
                return null;
            }).filter(Boolean);
    
            if (parsedResults.length !== keys.length) {
                throw new Error(`Txin lookup failed: txid=${txid}, expected_keys=${keys.length}, got_results=${parsedResults.length}, inputs=${JSON.stringify(inputs)}`);
            }
    
            return parsedResults;
        } catch (error) {
            console.error("Error fetching inputs from Redis:", error);
            throw error;
        }
    }
    
        
    async updateBlockMetadata(blockcount, timestamp) {
        await this.redisClient.set("block_count", String(blockcount));
    }
    
    async getLatestBlockcount() {
        const result = await this.redisClient.get("block_count");
        return result ? parseInt(result, 10) : 0;
    }
    
}

export default DynamoDB;
