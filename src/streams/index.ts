import 'source-map-support/register'

import * as BbPromise from 'bluebird';
import * as _ from 'lodash';
import * as fs from 'fs';

const standardOutSSL = require('single-line-log').stdout;

/*
    - How do a handle batching related messages together?
    - How do I cancel of flush when lambda is running out
        - Destory?
        - Final?
 */

import { Readable, Writable, Transform } from 'stream';

class ReadStream extends Readable {
    counter = 0;

    state: string | null = 'nul';

    constructor(params) {
        super({ objectMode: true, highWaterMark: 6 });

        this.on('close', () => {
            // console.log('\non close listener called in reader');
            this.state = 'cls'
        });

        this.on('end', () => {
            // console.log('\non end listener called in reader');
            this.state = 'end'
        });

        this.on('error', err => {
            // console.log('\non error listener called in reader', err);
            this.state = 'err'
        });
    }

    // Size is how many values
    async _read(size) {
        await BbPromise.delay(500);

        this.push(this.counter);
        ++this.counter;

        if (this.counter === 30) {
            this.emit('error', 'Some error message');
        }

        if (this.counter === 30) {
            // console.log('\nPushing Finish');
            this.push(null);
        }
    }

    async _destroy(err, callback) {

    }
}

class TransformStream extends Transform {
    state: string | null = 'nul';

    constructor(params) {
        super({ objectMode: true, highWaterMark: 2 });

        // First
        this.on('finish', () => {
            // console.log('\non finish listerner is called in transformer')
            this.state = 'fin'
        });

        this.on('close', () => {
            // console.log('\non close listener called in transformer');
            this.state = 'cls'
        });

        this.on('end', () => {
            // console.log('\non end listener called in transformer');
            this.state = 'end'
        });

        this.on('drain', () => {
            // console.log('\non drain listerner is called in transformer');
            this.state = 'drn'
        });

        this.on('error', err => {
            // console.log('\non error listener called in transformer', err);
            this.state = 'err'
        });
    }

    async _transform(item, encoding, callback) {    
        await BbPromise.delay(500);

        this.push(item);

        callback();
    }

    // Delays the 'finish' until callback is called -- close or write the remaining buffer
    async _final(callback) {

    }

    // Called externally
    async _destroy(err, callback) {

    }

    async _flush(callback) {

    }
}


class WriteStream extends Writable {
    file = fs.createWriteStream('./file.txt');

    state: string | null = 'nul';

    constructor(params) {
        super({ objectMode: true, highWaterMark: 4 });

        this.on('close', () => {
            // console.log('\non close listener called in writer');
            this.state = 'cls'
        });

        this.on('finish', () => {
            // console.log('\non finish listerner is called in writer')
            this.file.end();
            this.state = 'fin'
        });

        this.on('drain', () => {
            // console.log('\non drain listerner is called in writer');
            this.state = 'drn'
        });

        this.on('error', err => {
            // console.log('\non error listener called in writer', err);
            this.state = 'err'
        });
    }

    async _write(item, encoding, callback) {
        await BbPromise.delay(1000);
        
        this.file.write(item + '\n');

        callback();
    }

    // Delays the 'finish' until callback is called -- close or write the remaining buffer
    async _final(callback) {

    }

    // Called externally
    async _destroy(err, callback) {

    }
}

console.log(`${process.version}`)

const readableStream = new ReadStream({})
const transformStream1 = new TransformStream({})
const transformStream2 = new TransformStream({}) 
const writeStream = new WriteStream({});

readableStream
    .pipe(transformStream1)
    .pipe(transformStream2)
    .pipe(writeStream);

(async () => {
    while (true) {
        standardOutSSL(`
            (${readableStream.readableLength}) -> (${transformStream1.writableLength}, ${transformStream1.readableLength}) -> (${transformStream2.writableLength}, ${transformStream2.readableLength}) -> (${writeStream.writableLength})
            (${readableStream.state}) -> (${transformStream1.state}) -> (${transformStream2.state}) -> (${writeStream.state})
        `);

        await BbPromise.delay(1);
    }
})();