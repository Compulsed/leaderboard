import * as _ from 'lodash';
import * as AWS from 'aws-sdk';
import * as BbPromise from 'bluebird';

const docClient = new AWS.DynamoDB.DocumentClient();

import { Semaphore, semaphoreKey, semaphoreTableName } from './semaphore-model';

const LEASE_DURATION = 60 * 1000 // 60 seconds
const MAX_ATTEMPTS = 3;

const tryObtain = async (attemptNumber = 0) => {
    if (attemptNumber === MAX_ATTEMPTS) {
        return Promise.reject(`Failed to acquire Semaphore after ${MAX_ATTEMPTS} attempts`);
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
const selectSemaphore = (semaphoreList: Semaphore[]) => {
    const optionalSemaphore = _(semaphoreList)
        .filter(semaphore => Date.now() > (semaphore.expires || 0))
        .shuffle()
        .first();

    return optionalSemaphore || null;
};

/*
    Possibilities to handle since querying
        - Semaphore is no longer there since querying (worker takes the semaphore away or tombstone)
        - Semaphore might have a lease
*/
const attemptTakeSemaphore = async semaphore => {
    const now = Date.now();

    const params = {
        TableName: semaphoreTableName,
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
            ':expires': now + LEASE_DURATION,
            // ':currentTime': now,
            ':null': null,
        },
        ReturnValues: 'ALL_NEW'
    };

    console.log(
        'Attempting to acquire lease on semaphore: ',
        JSON.stringify(params, null, 2)
    );

    try {
        const updateResult = await docClient
            .update(params)
            .promise();

        const acquiredSemaphore = Object.assign({}, semaphore, updateResult.Attributes);

        console.log('Successfully Accquired Semaphore', console.log(JSON.stringify({ acquiredSemaphore }, null, 2)));

        return acquiredSemaphore
    } catch (err) {
        console.error('Error Acquiring Semaphore: ', err);

        return null;
    }
};

const releaseSemaphore = async semaphore => {
    const params = {
        TableName: semaphoreTableName,
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
        JSON.stringify(params, null, 2)
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
        TableName: semaphoreTableName,
        KeyConditionExpression: 'semaphore_key = :hkey',
        ExpressionAttributeValues: {
            ':hkey': semaphoreKey,
        },
    };
    
    console.log(
        'Querying Semaphores: ',
        JSON.stringify({ params }, null, 2)
    );

    const readResult = await docClient
        .query(params)
        .promise();

    console.log(JSON.stringify({ readResult }));

    return (readResult.Items || []) as Semaphore[];
};

export const countFreeSemaphores = async () => {
    const allSemaphores = await querySemaphores();

    const availableSemaphores = _.filter(
        allSemaphores, semaphore => Date.now() > (semaphore.expires || 0)
    );

    return availableSemaphores.length;
}

export const obtainSemaphore = () => {
    return BbPromise.resolve(tryObtain())
        .disposer(acquiredSemaphore => {
            return releaseSemaphore(acquiredSemaphore);
        });
};