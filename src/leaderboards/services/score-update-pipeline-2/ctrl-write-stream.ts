import { Writable } from 'stream';

import * as BbPromise from 'bluebird';
import * as _ from 'lodash';
import * as AWS from 'aws-sdk';
import * as uuid from 'uuid/v4';

// Model
import { ScoreUpdate, LeaderboardRecord} from '../../model';

// Config
import { LAMBDA_CHUNCK_SIZE } from '../../config';

// Setup
const lambda = new AWS.Lambda();
const updateFunctionName = process.env.UPDATE_FUNCTION || 'kinesis-leaderboard-dev-scoreStreamWorkerHandle'; // TODO

export class WriteStream extends Writable {
    constructor(params) {
        super({ objectMode: true, highWaterMark: 5000 });
    }

    fanoutUpdates (scoreUpdates: ScoreUpdate[]) {
        const chunkedScores = _.chunk(scoreUpdates, LAMBDA_CHUNCK_SIZE)
    
        const scoreUpdatePromises = chunkedScores
            .map(this.invokeWithScoreUpdates);
    
        // Logging
        scoreUpdatePromises
            .map(promise => promise
                .then(result => console.log(`Done taskId ${result.taskId} - ${result.records && result.records.length} records`))
            );
    
        return BbPromise.all(scoreUpdatePromises)
            .then(leaderboardRecord => _.flatten(leaderboardRecord));
    }
    
    invokeWithScoreUpdates (scoreUpdates: ScoreUpdate[]): Promise<{ taskId: string, records: LeaderboardRecord[]}> {
        const taskId = uuid() as string;
    
        const params = {
            FunctionName: updateFunctionName, 
            InvocationType: 'RequestResponse', 
            Payload: JSON.stringify({ taskId, scoreUpdates })
        };
    
        return lambda.invoke(params)
            .promise()
            .then(result => ({ taskId, records: JSON.parse(result.Payload as string)}));
    }
    
    async _write(item, encoding, callback) {
        this._writev([{ chunk: item, encoding }], callback);
    }

    async _writev(chunks, callback) {
        const scoreUpdates: ScoreUpdate[] = _.map(chunks, 'chunk');

        console.log(`Writing: ${scoreUpdates.length} updates`)

        // TODO: Consider buffering
        await this.fanoutUpdates(scoreUpdates);

        console.log(`Written: ${scoreUpdates.length} updates`)

        callback();
    }

    // TODO
    async _final(callback) {
        callback();
    }
}