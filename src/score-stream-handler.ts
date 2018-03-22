import 'source-map-support/register'
import * as uuid from 'uuid/v4';
import { Context, Callback } from 'aws-lambda';
import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

// Model
import { TimeInterval } from './leaderboards/model';
import { InputScoreRecord } from './leaderboards/services/write-leaderboard';

// Functions
import { getScoreUpdates } from './leaderboards/services/write-leaderboard';

// Defines the amount of update tasks that can be running at one time,
//  an update task involves (1 Read & 1 Write)
const recordConcurrencyLevel = 20;

const timeIntervals = [
    TimeInterval.HOUR,
    TimeInterval.DAY,
    TimeInterval.WEEK,
    TimeInterval.MONTH,
    TimeInterval.YEAR,
    TimeInterval.ALL_TIME,
];

const formatKinesisRecords = (kinesisRecords) => {
    return kinesisRecords
        .map(record => Buffer.from(record.kinesis.data, 'base64').toString())
        .map(recordJSONString => JSON.parse(recordJSONString))
        .map(_.property('data')) as InputScoreRecord[]
}

// Total Batch Duration
//  -> ((Tags) * (Facets) * (TimeIntervals) * (Batch Size) / MIN(RCU, WCU))
//  -> ((3 * 2 * 6 * 10) / 5)
//  -> 72 seconds
export const handler = async (event: any, context: Context, cb: Callback) => {
    console.log('event', JSON.stringify({ event }, null, 2));

    try {
        const formattedRecords = formatKinesisRecords(event.Records);

        const scoreUpates = getScoreUpdates(
            formattedRecords,
            timeIntervals
        );
        
        const processedRecords = await BbPromise.map(
            scoreUpates,
            scoreUpdate => scoreUpdate(),
            { concurrency: recordConcurrencyLevel }
        );

        console.log(JSON.stringify({
            fomattedRecords: formattedRecords.length,
            scoresWritten: processedRecords.length,
        }, null, 2));

        return cb(undefined, { message: 'Success' });
    } catch (err) {
        return console.error(err.message, err.stack) || cb(err);
    }
};
