import 'source-map-support/register';

import * as AWS from 'aws-sdk';
import * as _ from 'lodash';
import * as BbPromise from 'bluebird';

const kinesis = new AWS.Kinesis();

const randomNumber = max =>
    Math.floor(Math.random() * max);

const generateRecord = index => {
    const key = randomNumber(100);

    const data = { key, index };      

    const record = {
        PartitionKey: String(key),
        Data: JSON.stringify(data),
    }

    return record;
};

const putRecords = async () => {
    const records = _.times(10, generateRecord);

    const putRecords = {
        StreamName: 'ScoreStream',
        Records: records,
    };

    const kinesisResult = await kinesis
        .putRecords(putRecords)
        .promise();

    console.log(JSON.stringify({ kinesisResult }, null, 2));
};

export const handler = async (event, context, cb) => {
    console.log('In handler!');

    await BbPromise.map(
        _.times(10),
        putRecords,
        { concurrency: 5 },
    );

    cb(undefined, { message: 'In Message returned of Lambda'});
};