class BCDataStream {
  constructor(buffer) {
    this.buffer = buffer;
    this.offset = 0;
  }

  checkBounds(length) {
    if (this.offset + length > this.buffer.length) {
      throw new Error(`Buffer underflow: trying to read ${length} bytes, but only ${this.buffer.length - this.offset} bytes remaining`);
    }
  }

  readUInt32LE() {
    this.checkBounds(4);
    const value = this.buffer.readUInt32LE(this.offset);
    this.offset += 4;
    return value;
  }

  readUInt8() {
    this.checkBounds(1);
    const value = this.buffer.readUInt8(this.offset);
    this.offset += 1;
    return value;
  }

  readUInt64LE() {
    this.checkBounds(8);
    const value = this.buffer.readBigUInt64LE(this.offset);
    this.offset += 8;
    return value;
  }

  slice(length) {
    this.checkBounds(length);
    const slice = this.buffer.slice(this.offset, this.offset + length);
    this.offset += length;
    return slice;
  }

  reverseSlice(length) {
    const slice = this.slice(length).reverse();
    return slice;
  }

  resetOffset() {
    this.offset = 0;
  }

  parseBlock() {
    const block = {};

    block.version = this.readUInt32LE();
    block.previousBlockHash = this.reverseSlice(32).toString('hex');
    block.merkleRoot = this.reverseSlice(32).toString('hex');
    block.timestamp = this.readUInt32LE();
    block.difficultyTarget = this.slice(4).toString('hex');
    block.nonce = this.readUInt32LE();

    const transactionCount = this.readUInt8();
    block.transactions = [];
    for (let i = 0; i < transactionCount; i++) {
      const tx = this.parseTransaction();
      block.transactions.push(tx);
    }

    return block;
  }

  parseTransaction() {
    const tx = {};

    tx.version = this.readUInt32LE();
    const inputCount = this.readUInt8();
    tx.inputs = [];
    for (let i = 0; i < inputCount; i++) {
      const input = this.parseInput();
      tx.inputs.push(input);
    }

    const outputCount = this.readUInt8();
    tx.outputs = [];
    for (let i = 0; i < outputCount; i++) {
      const output = this.parseOutput();
      tx.outputs.push(output);
    }

    tx.lockTime = this.readUInt32LE();

    return tx;
  }

  parseInput() {
    const input = {};
  
    // Ensure there's enough data for previous output (32 bytes)
    this.checkBounds(32);
    input.previousOutput = this.reverseSlice(32).toString('hex');
  
    const scriptLength = this.readUInt8();  // Script length byte
    console.log(`Script length: ${scriptLength}`);
  
    // Check if the script length is valid (i.e., within the remaining buffer)
    if (scriptLength > (this.buffer.length - this.offset)) {
      console.warn(`Warning: Script length of ${scriptLength} exceeds remaining buffer size. Skipping input.`);
      return null;  // Return null or handle error as appropriate
    }
  
    this.checkBounds(scriptLength);
    input.script = this.slice(scriptLength).toString('hex');
    input.sequence = this.readUInt32LE();
  
    return input;
  }
  parseOutput() {
    const output = {};

    output.value = this.readUInt64LE();
    const scriptLength = this.readUInt8();

    // Check if the script length is valid
    if (scriptLength > (this.buffer.length - this.offset)) {
      throw new Error(`Script length of ${scriptLength} exceeds remaining buffer size`);
    }

    this.checkBounds(scriptLength);
    output.script = this.slice(scriptLength).toString('hex');

    return output;
  }
}

module.exports = { BCDataStream };
