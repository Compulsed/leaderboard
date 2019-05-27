const AWS = require('aws-sdk');
const _ = require('lodash');
const BbPromise = require('bluebird');
const uuidv4 = require('uuid/v4');

const sqs = new AWS.SQS();

const QUEUE_URL = process.env.QUEUE_URL;

// Total Items = 10 * 1000
const noRecords = 10;

const randomNumber = max =>
    Math.floor(Math.random() * max);

const generateRecord = index => {
    const key = String(randomNumber(10000));

    const data = {
        userId: key,
        score: 1,
        date: Date.now().toString(),
        inputFacets: {
            tags: ['aws', 'ec2'],
            locations: ['melbourne', 'australia'],
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
    const records = _.times(noRecords, generateRecord);

    const sqsEnqueueReponse = {
        QueueUrl: QUEUE_URL,
        Entries: records,
    };

    const enqueueResponse = await sqs
        .sendMessageBatch(sqsEnqueueReponse)
        .promise();

    console.log(JSON.stringify({ enqueueResponse }));
};

const handler = async (event, context, cb) => {
    const noRecordSets = Math.floor((event.records / 10)) || 1000;

    console.log('In handler!', JSON.stringify({ event }));

    await BbPromise.map(
        _.times(noRecordSets),
        putRecords,
        { concurrency: 10 },
    );

    cb(undefined, { message: 'In Message returned of Lambda'});
};

module.exports = { handler };