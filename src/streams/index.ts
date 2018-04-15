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

const loggerWriteStream = _.memoize(fileName => 
    fs.createWriteStream(`./stream/${fileName}.txt`, { flags: 'w' }))

const log = (file, message) => {
    loggerWriteStream(file).write(message);
}

class ReadStream extends Readable {
    counter = 0;

    state: string | null = 'nul';

    constructor(params) {
        super({ objectMode: true, highWaterMark: 6 });

        this.on('close', () => {
            log('read', 'on close listener called in reader\n');
            this.state = 'cls'
        });

        this.on('end', () => {
            log('read', 'on end listener called in reader\n');
            this.state = 'end'
        });

        this.on('error', err => {
            log('read', 'on error listener called in reader\n');
            this.state = 'err'
        });
    }

    // Size is how many values
    async _read(size) {
        log('size', `Size: ${size}\n`);

        if (this.state !== 'cls' && this.state !== 'dst') {
            await BbPromise.delay(500);

            this.push(this.counter);
            ++this.counter;

            // if (this.counter === 30) {
            //     this.emit('error', 'Some error message'); // cloud also emit `close` / `destory`
            // }
        }

        if (this.counter === 30 || this.state === 'cls' || this.state === 'dst') {
            this.push(null); // Tells the readstream to stop calling _read, and propagates the 'end'
        }
    }

    async _destroy(err, callback) {
        this.state = 'dst';

        await BbPromise.delay(3000);

        callback();
    }
}

class TransformStream extends Transform {
    state: string | null = 'nul';
    transformNumber;

    constructor(params) {
        super({ objectMode: true, highWaterMark: 2 });

        this.transformNumber = params.transformNumber || 0;

        // First
        this.on('finish', () => {
            log(`transform-${this.transformNumber}`, 'on finish listerner is called in transformer\n')
            // this.state = 'fns';
        });

        this.on('close', () => {
            log(`transform-${this.transformNumber}`, 'on close listener called in transformer\n');
            this.state = 'cls'
        });

        this.on('end', () => {
            log(`transform-${this.transformNumber}`, 'on end listener called in transformer\n');
            this.state = 'end'
        });

        this.on('drain', () => {
            log(`transform-${this.transformNumber}`, 'on drain listerner is called in transformer\n');
            this.state = 'drn'

            this.once('drain', () => {
                this.state = 'nul' 
            });
        });

        this.on('error', err => {
            log(`transform-${this.transformNumber}`, 'on error listener called in transformer\n');
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
        this.state = 'fnl';

        await BbPromise.delay(3000);

        callback(); // sets the stream to flush vvv
    }

    // Flush & the finish event are called almost at the same time
    // -> final
    // -> (flush -> finish)
    // -> end
    async _flush(callback) {
        this.state = 'fsh';

        await BbPromise.delay(10000);

        callback(); // sets the steam to end
    }

    // Called externally
    async _destroy(err, callback) {
        this.state = 'dst';

        await BbPromise.delay(3000);

        callback();
    }
}


class WriteStream extends Writable {
    file = fs.createWriteStream('./file.txt');

    state: string | null = 'nul';

    constructor(params) {
        super({ objectMode: true, highWaterMark: 4 });

        this.on('close', () => {
            log('write', 'on close listener called in writer\n');
            this.state = 'cls'
        });

        this.on('finish', () => {
            log('write', 'on finish listerner is called in writer\n');
            this.file.end();
            this.state = 'fns';
        });

        this.on('drain', () => {
            log('write', 'on drain listerner is called in writer\n');
            this.state = 'drn';

            this.once('drain', () => {
                this.state = 'nul' 
            });
        });

        this.on('error', err => {
            log('write', 'on error listener called in writer\n');
            this.state = 'err'
        });
    }

    async _write(item, encoding, callback) {
        await BbPromise.delay(1000);
        
        this.file.write(item + '\n');

        callback();
    }

    async _writev(chunks, callback) {
        await BbPromise.map(chunks, async ({ chunk }) => {
            await BbPromise.delay(1000);
        
            this.file.write(`${chunk} - ${JSON.stringify(chunks.map(_.property('chunk')))}\n`);
        });

        callback();
    }

    // Delays the 'finish' until callback is called -- close or write the remaining buffer
    async _final(callback) {
        this.state = 'fnl';
        
        await BbPromise.delay(3000);

        callback();
    }

    // Called externally
    async _destroy(err, callback) {
        this.state = 'dst';

        await BbPromise.delay(3000);

        callback();
    }
}

const readableStream = new ReadStream({})
const transformStream1 = new TransformStream({ transformNumber: 1 })
const transformStream2 = new TransformStream({ transformNumber: 2 }) 
const writeStream = new WriteStream({});

readableStream
    .pipe(transformStream1)
    .pipe(transformStream2)
    .pipe(writeStream);

(async () => {
    await BbPromise.delay(10 * 1000);

    readableStream.destroy();
});

(async () => {
    await BbPromise.delay(10 * 1000);

    readableStream.emit('close')
});

(async () => {
    while (true) {
        standardOutSSL(`
            (${readableStream.readableLength}) -> (${transformStream1.writableLength}, ${transformStream1.readableLength}) -> (${transformStream2.writableLength}, ${transformStream2.readableLength}) -> (${writeStream.writableLength})
            (${readableStream.state}) -> (${transformStream1.state}) -> (${transformStream2.state}) -> (${writeStream.state})
        `);

        await BbPromise.delay(1);
    }
})();