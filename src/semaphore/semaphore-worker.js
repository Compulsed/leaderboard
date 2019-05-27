const _ = require('lodash');
const AWS = require('aws-sdk');
const uuidv4 = require('uuid/v4');
const BbPromise = require('bluebird');

const SEMAPHORE_TABLE_NAME = process.env.SEMAPHORE_TABLE || 'Unknown';
const LEADERBOARD_TABLE_NAME = process.env.LEADERBOARD_TABLE || 'Unknown';
const SEMAPHORE_KEY = process.env.SEMAPHORE_KEY || 'semaphore';
const WORKER_WRITE_SPEED = parseInt(process.env.WORKER_WRITE_SPEED, 10) || 100;

const { docClient } = require('../aws/document-client');

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

const queryTableCapacity = async () => {
    const DynamoDB = new AWS.DynamoDB();

    const readResult = await DynamoDB
        .describeTable({ TableName: LEADERBOARD_TABLE_NAME })
        .promise();

    console.log(JSON.stringify({ readResult }))
        
    const capacity = Math.min(
        _.get(readResult, 'Table.ProvisionedThroughput.ReadCapacityUnits'),
        _.get(readResult, 'Table.ProvisionedThroughput.WriteCapacityUnits')
    );

    return capacity;      
};

const increaseSemaphores = async increaseAmount => {
    console.log(`Increase Semaphores: ${increaseAmount}`);

    const addSemaphore = () => {
        const putParams = {
            TableName: SEMAPHORE_TABLE_NAME,
            Item: {
                semaphore_key: SEMAPHORE_KEY,
                semaphore_sort_key: uuidv4(),
                expires: null,
            },
        };
    
        console.log('Putting Semaphores: ', JSON.stringify({ putParams }))

        return docClient
            .put(putParams)
            .promise()
    }

    await BbPromise.all(_.times(increaseAmount, addSemaphore));
};

// Because we are re-querying, 
const decreaseSemaphores = async decreaseAmount => {
    console.log(`Decrease Semaphores: ${decreaseAmount}`);

    const deleteSemaphore = semaphore => {
        const deleteParams = {
            TableName: SEMAPHORE_TABLE_NAME,
            Key: {
                semaphore_key: semaphore.semaphore_key,
                semaphore_sort_key: semaphore.semaphore_sort_key,
            },
        };
    
        console.log('Taking Semaphores: ', JSON.stringify({ deleteParams }))

        return docClient
            .delete(deleteParams)
            .promise();
    }

    const semaphores = await querySemaphores();

    // Marked for deletion is first, then the ones who expire next
    const orderedSemaphores = _(semaphores)
        .sortBy(['expires'])
        .reverse()
        .value()

    const semaphoresToRemove = orderedSemaphores.slice(0, decreaseAmount - 1);

    await BbPromise.all(semaphoresToRemove.map(deleteSemaphore));
};

const updateSemaphores = async (currentSemaphoreCount, recommendedSemaphoreCount) => {
    if (currentSemaphoreCount > recommendedSemaphoreCount) {
        await decreaseSemaphores(currentSemaphoreCount - recommendedSemaphoreCount);
    } else {
        await increaseSemaphores(recommendedSemaphoreCount - currentSemaphoreCount);
    }
};

const adjustSemaphoreCount = async () => {
    const [currentSemaphores, capacity] = await BbPromise.all([querySemaphores(), queryTableCapacity()]);

    const recommendedSemaphoreCount = Math.ceil(capacity / WORKER_WRITE_SPEED);

    if (currentSemaphores.length !== recommendedSemaphoreCount) {
        await updateSemaphores(currentSemaphores.length, recommendedSemaphoreCount);
    }

    return;
};

module.exports = { adjustSemaphoreCount };