import * as _ from 'lodash';
import * as AWS from 'aws-sdk';
import * as uuidv4 from 'uuid/v4';

const docClient = new AWS.DynamoDB.DocumentClient();
const DynamoDB = new AWS.DynamoDB();

import { Semaphore, semaphoreKey, semaphoreTableName, leaderboardTableName } from './semaphore-model';

const WORKER_WRITE_SPEED = 200;

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

const queryTableCapacity = async () => {
    const readResult = await DynamoDB
        .describeTable({ TableName: leaderboardTableName })
        .promise();
        
    const capacity = Math.min(
        _.get(readResult, 'Table.ProvisionedThroughput.ReadCapacityUnits'),
        _.get(readResult, 'Table.ProvisionedThroughput.WriteCapacityUnits')
    );

    return capacity;      
}

const increaseSemaphores = async amount => {
    const addSemaphore = () => {
        const putParams = {
            TableName: semaphoreTableName,
            Item: {
                semaphore_key: semaphoreKey,
                semaphore_sort_key: uuidv4(),
                expires: null,
            },
        };
    
        return docClient
            .put(putParams)
            .promise()
    }

    await BbPromise.all(_.times(amount, addSemaphore));
}

const tombStoneSemaphores = async (semaphores) => {
    const params = {
        TableName: semaphoreTableName,
        Key: {
            semaphore_key: semaphore.semaphore_key,
            semaphore_sort_key: semaphore.semaphore_sort_key,
        },
        UpdateExpression: 'set #tomb_stone = :true',
        ExpressionAttributeNames: {
            '#tomb_stone': 'tomb_stone',
        },
        ExpressionAttributeValues: {
            ':true': true
        },
    };

    console.log(
        'Attempting to acquire lease on semaphore: ',
        JSON.stringify(params, null, 2)
    );

    try {
        const updateResult = await docClient
            .update(params)
            .promise();
    } catch (err) {
        return null;
    }

    await BbPromise.all(_.times(amount, addSemaphore));
}

// Because we are re-querying, 
const decreaseSemaphores = async amountToDecrease => {
    const semaphores = await querySemaphores();

    // Marked for deletion is first, then the ones who expire next
    const orderedSemaphores = _(semaphores)
        .sortBy(['tomb_stone', 'expires'])
        .reverse()
        .value();

    const tombStonedSemaphores = _.filter(orderedSemaphores, 'tomb_stone');

    if (amountToDecrease < tombStonedSemaphores.length) {
        const nonTombstonedSemaphores = _(semaphores)
            .sortBy('expires')
            .filter(semaphore => semaphore.tomb_stone !== true)
            .reverse()
            .value();    
        
        // TODO: Check logic
        tombStoneSemaphores(nonTombstonedSemaphores.slice(0, amountToDecrease - tombStonedSemaphores.length))
    }
}

const updateSemaphores = async (currentSemaphoreCount, recommendedSemaphoreCount) => {
    if (currentSemaphoreCount > recommendedSemaphoreCount) {
        await decreaseSemaphores(currentSemaphoreCount - recommendedSemaphoreCount);
    } else {
        await increaseSemaphores(recommendedSemaphoreCount - currentSemaphoreCount);
    }
}

const adjustSemaphoreCount = async () => {
    const [currentSemaphores, capacity] = await BbPromise.all([querySemaphores(), queryTableCapacity()]);

    const recommendedSemaphoreCount = Math.ceil(capacity / WORKER_WRITE_SPEED);

    if (currentSemaphores.length !== recommendedSemaphoreCount) {
        await updateSemaphores(currentSemaphores.length, recommendedSemaphoreCount);
    }

    return;
}