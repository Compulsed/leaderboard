import 'source-map-support/register';

import * as AWS from 'aws-sdk';
import * as _ from 'lodash';
import * as BbPromise from 'bluebird';

import { InputScoreUpdate } from './leaderboards/model';

const kinesis = new AWS.Kinesis();

const noRecords = 500
const noRecordSets = 20

const randomNumber = max =>
    Math.floor(Math.random() * max);

const generateRecord = index => {
    const key = String(randomNumber(50));

    const data: InputScoreUpdate = {
        userId: key,
        score: 1,
        date: Date.now().toString(),
        inputFacets: {
            tags: ['aws', 'ec2'],
            organisationIds: ['org1'],
            locations: ['melbourne', 'australia'],
        },
    };

    const record = {
        PartitionKey: key,
        Data: JSON.stringify({ data }),
    }

    return record;
};

const putRecords = async () => {
    // await BbPromise.delay(100);

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
        { concurrency: 1 },
    );

    cb(undefined, { message: 'In Message returned of Lambda'});
};