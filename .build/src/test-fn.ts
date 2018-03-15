import 'source-map-support/register';

import * as AWS from 'aws-sdk';
import * as _ from 'lodash';
import * as BbPromise from 'bluebird';

const kinesis = new AWS.Kinesis();

const noRecords = 100
const noRecordSets = 100

const randomNumber = max =>
    Math.floor(Math.random() * max);

const generateRecord = index => {
    const key = String(randomNumber(5));

    const data = { userId: key, score: 1, organisationId: 'test-org', location: 'test-loc' };      

    const record = {
        PartitionKey: key,
        Data: JSON.stringify({ data }),
    }

    return record;
};

const putRecords = async () => {
    const records = _.times(noRecords, generateRecord);

    const putRecords = {
        StreamName: 'kinesis-leaderboard-dev-scoreStream',
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
        _.times(noRecordSets),
        putRecords,
        { concurrency: 5 },
    );

    cb(undefined, { message: 'In Message returned of Lambda'});
};