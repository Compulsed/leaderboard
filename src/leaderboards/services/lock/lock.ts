const BbPromise = require('bluebird');
const AWS = require('aws-sdk');
const documentClient = new AWS.DynamodocumentClient.DocumentClient();

const lockKey = 'score-queue-lock';
const lockTable = process.env.LOCK_TABLE_NAME || 'lockTable';

const createLock = () => {
    const nowInSeconds = Math.floor((Date.now() / 1000));

    const timeoutDurationInSeconds = 5 * 60 * 1000; // 5 minutes

    console.log(`Timeout duration: ${timeoutDurationInSeconds}`)

    const expiryInSeconds = nowInSeconds + timeoutDurationInSeconds;

    const params = {
        TableName: lockTable,
        Key: { lock_key: lockKey, },
        ConditionExpression: 'attribute_not_exists(lock_key) or :currentTime > #expires',
        UpdateExpression: 'set #expires = :expires',
        ExpressionAttributeNames: {
            '#expires': 'expires',
        },
        ExpressionAttributeValues: {
            ':expires': expiryInSeconds,
            ':currentTime': nowInSeconds
        },
    };

    console.log('Creating subscription lock', JSON.stringify(params));

    return documentClient
        .update(params)
        .promise();
};

const removeLock = () => {
    const params = {
        TableName: lockTable,
        Key: { lock_key: lockKey, },
    };

    console.log('Removing subscription lock', JSON.stringify(params));

    return documentClient
        .delete(params)
        .promise();
};

const obtainLock = () => {
    return BbPromise.resolve(createLock()).disposer(() => {
        return removeLock().catch((err) => {
            console.log('Error removing lock', err);
            return Promise.reject(err);
        });
    });
};