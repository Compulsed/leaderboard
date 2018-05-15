import * as _ from 'lodash';
import * as AWS from 'aws-sdk';
import * as uuidv4 from 'uuid/v4';
import * as BbPromise from 'bluebird';

const docClient = new AWS.DynamoDB.DocumentClient();
const DynamoDB = new AWS.DynamoDB();

import { Semaphore, semaphoreKey, semaphoreTableName, leaderboardTableName } from './semaphore-model';

const WORKER_WRITE_SPEED = 100;

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

    console.log(JSON.stringify({ readResult }, null, 2))
        
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
            TableName: semaphoreTableName,
            Item: {
                semaphore_key: semaphoreKey,
                semaphore_sort_key: uuidv4(),
                expires: null,
            },
        };
    
        console.log('Putting Semaphores: ', JSON.stringify({ putParams }, null, 2))

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
            TableName: semaphoreTableName,
            Key: {
                semaphore_key: semaphore.semaphore_key,
                semaphore_sort_key: semaphore.semaphore_sort_key,
            },
        };
    
        console.log('Taking Semaphores: ', JSON.stringify({ deleteParams }, null, 2))

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

export const adjustSemaphoreCount = async () => {
    const [currentSemaphores, capacity] = await BbPromise.all([querySemaphores(), queryTableCapacity()]);

    const recommendedSemaphoreCount = Math.ceil(capacity / WORKER_WRITE_SPEED);

    if (currentSemaphores.length !== recommendedSemaphoreCount) {
        await updateSemaphores(currentSemaphores.length, recommendedSemaphoreCount);
    }

    return;
};