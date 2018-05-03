import 'source-map-support/register'
import { Context, Callback } from 'aws-lambda';

import * as BbPromise from 'bluebird';

import { TransformStream } from './leaderboards/services/score-update-pipeline-2/ctrl-fanout-stream';
import { ReadStream } from './leaderboards/services/score-update-pipeline-2/ctrl-fifo-stream';
import { WriteStream } from './leaderboards/services/score-update-pipeline-2/ctrl-write-stream';

export const handler = async (event: any, context: Context, cb: Callback) => {
    const readStream = new ReadStream({ 
        timeLeftFunction: context.getRemainingTimeInMillis
    });

    const transformStream = new TransformStream({});

    const writeStream = new WriteStream({});
    
    readStream
        .pipe(transformStream)
        .pipe(writeStream)
    
    writeStream.once('close', () => cb());
};