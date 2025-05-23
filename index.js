import axios from 'axios';
import fs from 'fs';
import dotenv from 'dotenv';
import { performance } from 'perf_hooks';
import DynamoDB from './dynamodb.js';
import path, { dirname } from 'path';
import { fileURLToPath } from 'url';
// Initialize environment variables
dotenv.config();

// Get the directory name equivalent to __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function storeTimings(blockNumber, totalTime, getBlockTime, putDataTime) {
    const filePath = path.join(__dirname, 'indexing_timings.csv');
    const fileExists = fs.existsSync(filePath);
    const currentTime = new Date().toISOString().replace('T', ' ').replace('Z', '');

    const headers = ["Date_Time", "Block Number", "Total Time", "Get Block Time", "Put Data Time"];
    const row = [currentTime, blockNumber, totalTime, getBlockTime, putDataTime];

    const dataToWrite = (fileExists ? '' : `${headers.join(",")}\n`) + row.join(",") + "\n";

    await fs.promises.appendFile(filePath, dataToWrite, 'utf8');
}

class Indexer {
    constructor() {
      this.avg = 0;
      this.itercount = 0;
    }

    // Define getBlockHashes method to retrieve block hashes
    async getBlockHashes(startBlock, endBlock) {
        const blockHashes = [];
        for (let cnt = startBlock; cnt < endBlock; cnt++) {
          const response = await axios.post(process.env.BITCOIND_ENDPOINT, {
            method: 'getblockhash',
            params: [cnt],
          });
          blockHashes.push(response.data.result);
        }
        return blockHashes;
    }

    async getBlockData(blockHashes) {
        const { default: pLimit } = await import('p-limit');
        const limit = pLimit(8);
        const blockDataPromises = blockHashes.map((blkHash) =>
            limit(async () => {
                const response = await axios.post(process.env.BITCOIND_ENDPOINT, {
                    method: 'getblock',
                    params: [blkHash, 2],
                });
                return response.data.result;
            })
        );
        return Promise.all(blockDataPromises);
    }

    async doIndex(dynamodb) {
        console.log("Indexer Running");
        const rpcNode = axios.create({ baseURL: process.env.BITCOIND_ENDPOINT });

        const totalCountResponse = await rpcNode.post('', {
            method: 'getblockcount',
            params: [],
        });

        this.totalcount = totalCountResponse.data.result;
        const latestBlockCountResponse = await dynamodb.getLatestBlockcount();
        this.blockcount = latestBlockCountResponse || 0;

        let start = performance.now();
        let getBlockTime = 0, parseDataTime = 0, putDataTime = 0;

        while (this.blockcount < this.totalcount - 1) {
            let start1 = performance.now();
            this.fetchBlkBatch = parseInt(process.env.FETCH_BLK_BATCH || "100");
            const endBlock = Math.min(this.blockcount + this.fetchBlkBatch, this.totalcount - 1);
            const blockHashes = await this.getBlockHashes(this.blockcount, endBlock);
            if (blockHashes.length === 0) {
                console.log("Block data is empty!");
                break;
            }
            const blockData = await this.getBlockData(blockHashes);
            getBlockTime += performance.now() - start1;
            let putDataTimeDiff = 0;



            for (let idx = 0; idx < blockData.length; idx++) {
                const current_block = blockData[idx]
                let blockNumber = this.blockcount + idx;
                let start2 = performance.now();
                const tx_time = parseInt(current_block.time, 10);
                await dynamodb.storeTransactions(current_block, tx_time);
                putDataTimeDiff += performance.now() - start2;
                console.log(`Processed block ${blockNumber} with ${current_block.tx.length} transactions`);
                await dynamodb.updateBlockMetadata(blockNumber, tx_time);
                if (blockNumber % 1000 === 0 && blockNumber > 0) {
                    const totalElapsedTime = (performance.now() - start) / 1000; // total since start
                    await storeTimings(
                        blockNumber,
                        totalElapsedTime.toFixed(3),
                        (getBlockTime / 1000).toFixed(3),
                        (putDataTime / 1000).toFixed(3)
                    );
                    console.log(`Saved timing info at block ${this.blockcount}`);
                }
            }

            // Increment block count after processing all blocks in the batch
            this.blockcount += blockData.length;
            let totalTime = performance.now() - start1;
            putDataTime += putDataTimeDiff;
            this.itercount += blockData.length;

            this.avg = (totalTime + (this.avg * (this.itercount - blockData.length))) / this.itercount;

            let estimatedTimeRemaining = (this.totalcount - this.blockcount) * this.avg;
            console.log(`Estimated time remaining: ${(estimatedTimeRemaining/1000).toFixed(3)} s`);
            console.log(`Total Time: ${(totalTime/1000).toFixed(3)}s | Get Block: ${(getBlockTime/1000).toFixed(3)}s | Put: ${(putDataTimeDiff/1000).toFixed(3)}s`);
        }
    }
}

  
  async function main() {
    const indexer = new Indexer();
  
    while (true) {
      const dynamodb = new DynamoDB();
      await indexer.doIndex(dynamodb);
      await new Promise(resolve => setTimeout(resolve, process.env.INDEXING_ATTEMP_INTERVAL)); // Sleep before next attempt
    }
  }
  
  main().catch(console.error);
  