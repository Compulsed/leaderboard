const _ = require('lodash');
const AWS = require('aws-sdk');
const BbPromise = require('bluebird');

const { docClient } = require('../aws/document-client');

const SEMAPHORE_TABLE_NAME = process.env.SEMAPHORE_TABLE || 'Unknown';
const SEMAPHORE_KEY = process.env.SEMAPHORE_KEY || 'semaphore';
const LEASE_DURATION = parseInt(process.env.LEASE_DURATION, 10) || 60 * 1000;
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS, 10) ||  1;

const tryObtain = async (attemptNumber = 0) => {
    console.log(`tryObtain called with attemptNumber: ${attemptNumber}`);

    if (attemptNumber >= MAX_ATTEMPTS) {

        return Promise.reject('FAILED_TO_ACQUIRE_SEMAPHORE');
    }

    // Apply backoff with some jitter
    await BbPromise.delay(attemptNumber * 100 * Math.random());

    const allSemaphores = await querySemaphores();

    const semaphoreToTryObtain = selectSemaphore(allSemaphores);

    if (semaphoreToTryObtain) {
        const acquiredSemaphore = await attemptTakeSemaphore(semaphoreToTryObtain);

        if (acquiredSemaphore) {
            return acquiredSemaphore;
        }
    }

    return tryObtain(attemptNumber + 1);
};

// Might return a semaphore
const selectSemaphore = (semaphoreList) => {
    const nowUnix = Math.floor(Date.now() / 1000);

    const optionalSemaphore = _(semaphoreList)
        .filter(semaphore => nowUnix > (semaphore.expires || 0))
        .shuffle()
        .first();

    return optionalSemaphore || null;
};

/*
    Note: Can only handle with expires is null, should be able to check if time now > expired time.
*/
const attemptTakeSemaphore = async semaphore => {
    const leaseDurationUnix = Math.floor((Date.now() + LEASE_DURATION) / 1000)


    const params = {
        TableName: SEMAPHORE_TABLE_NAME,
        Key: {
            semaphore_key: semaphore.semaphore_key,
            semaphore_sort_key: semaphore.semaphore_sort_key,
        },
        // NOTE: I think this logic is wrong
        ConditionExpression: `
            attribute_exists(semaphore_key) AND
            attribute_exists(semaphore_sort_key) AND
            (#expires = :null)
        `, // OR :currentTime > #expires)
        UpdateExpression: 'set #expires = :expires',
        ExpressionAttributeNames: {
            '#expires': 'expires',
        },
        ExpressionAttributeValues: {
            ':expires': leaseDurationUnix, // Must be in unix time
            ':null': null,
        },
        ReturnValues: 'ALL_NEW'
    };

    console.log(
        'Attempting to acquire lease on semaphore: ',
        JSON.stringify(params)
    );

    try {
        const updateResult = await docClient
            .update(params)
            .promise();

        const acquiredSemaphore = Object.assign({}, semaphore, updateResult.Attributes);

        console.log('Successfully Accquired Semaphore', JSON.stringify({ acquiredSemaphore }));

        return acquiredSemaphore
    } catch (err) {
        if (err && err.code !== 'ConditionalCheckFailedException') {
            console.error('Error Acquiring Semaphore: ', err); 
        }
        
        return null;
    }
};

const releaseSemaphore = async semaphore => {
    const params = {
        TableName: SEMAPHORE_TABLE_NAME,
        Key: {
            semaphore_key: semaphore.semaphore_key,
            semaphore_sort_key: semaphore.semaphore_sort_key,
        },
        // Only need release it if it still exists
        ConditionExpression: `
            attribute_exists(semaphore_key) AND
            attribute_exists(semaphore_sort_key)
        `,
        UpdateExpression: 'set #expires = :expires',
        ExpressionAttributeNames: {
            '#expires': 'expires',
        },
        ExpressionAttributeValues: {
            ':expires': null
        },
    };

    console.log(
        'Attempting to release lease on semaphore: ',
        JSON.stringify(params)
    );

    try {
        await docClient
            .update(params)
            .promise();

        console.log('Successfully released semaphore');
    } catch (err) {
        console.log('Failed to released semaphore');
    }
}

const querySemaphores = async () => {
    const params = {
        TableName: SEMAPHORE_TABLE_NAME,
        KeyConditionExpression: 'semaphore_key = :hkey',
        ExpressionAttributeValues: {
            ':hkey': SEMAPHORE_KEY,
        },
    };
    
    console.log(
        'Querying Semaphores: ',
        JSON.stringify({ params })
    );

    const readResult = await docClient
        .query(params)
        .promise();

    console.log(JSON.stringify({ readResult }));

    return (readResult.Items || []);
};

const countFreeSemaphores = async () => {
    const allSemaphores = await querySemaphores();

    const nowUnix = Math.floor(Date.now() / 1000);

    const availableSemaphores = _.filter(
        allSemaphores, semaphore => nowUnix > (semaphore.expires || 0)
    );

    console.log(`(TotalSemaphores: ${allSemaphores.length}, AvailableSemaphores: ${availableSemaphores.length})`);

    return availableSemaphores.length;
};

const obtainSemaphore = () => {
    return BbPromise.resolve(tryObtain(0))
        .disposer(acquiredSemaphore => {
            return releaseSemaphore(acquiredSemaphore);
        });
};

module.exports = { countFreeSemaphores, obtainSemaphore };