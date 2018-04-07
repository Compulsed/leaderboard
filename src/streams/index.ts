import * as BbPromise from 'bluebird';
import * as _ from 'lodash';


import { Readable, Writable, Transform } from 'stream';

class ReadStream extends Readable {
    counter = 0;

    constructor(params) {
        super({ objectMode: true, highWaterMark: 1000 });
    }

    async _read(size) {
        this.push(++this.counter);
    }
}

class TransformStream extends Transform {
    previous = null

    constructor(params) {
        super({ objectMode: true, highWaterMark: 1000 });
    }

    async _transform(item, encoding, callback) {    
        if (this.previous) {
            this.push(item + this.previous);
            this.clearPrevious();
        } else {
            this.previous = item;
        }

        callback();
    }

    clearPrevious() {
        this.previous = null;
    }
}


class WriteStream extends Writable {
    constructor(params) {
        super({ objectMode: true, highWaterMark: 1000 });
    }

    async _write(item, encoding, callback) {
        console.log(JSON.stringify({ item, encoding }));

        await BbPromise.delay(1000);
        
        callback();
    }
}

 new ReadStream({})
    .pipe(new TransformStream({}))
    .pipe(new WriteStream({}));