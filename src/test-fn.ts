import 'source-map-support/register';

import * as AWS from 'aws-sdk';
import * as _ from 'lodash';
import * as BbPromise from 'bluebird';
import * as uuidv4 from 'uuid/v4';

import { InputScoreUpdate } from './leaderboards/model';

const sqs = new AWS.SQS();

const queueUrl = 'https://sqs.us-east-1.amazonaws.com/145722906259/scoreQueue.fifo';

const noRecords = 5
const noRecordSets = 300

const randomNumber = max =>
    Math.floor(Math.random() * max);

const generateRecord = index => {
    const key = String(randomNumber(100));

    const data: InputScoreUpdate = {
        userId: key,
        score: 1,
        date: Date.now().toString(),
        inputFacets: {
            tags: ['aws', 'ec2'],
            organisationIds: ['org1'],
            locations: ['melbourne', 'australia'],
            cohorts: ['1', '2'],
        },
    };

    const record = {
        Id: uuidv4(),
        MessageBody: JSON.stringify(data),
        MessageGroupId: data.userId,
    };

    return record;
};

const putRecords = async () => {
    // await BbPromise.delay(100);

    const records = _.times(noRecords, generateRecord);

    const sqsEnqueueReponse = {
        QueueUrl: queueUrl,
        Entries: records,
    };

    const enqueueResponse = await sqs
        .sendMessageBatch(sqsEnqueueReponse)
        .promise();

    console.log(JSON.stringify({ enqueueResponse }, null, 2));
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