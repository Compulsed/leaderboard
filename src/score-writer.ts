import 'source-map-support/register'

import * as BbPromise from 'bluebird';
import * as _ from 'lodash';
import * as AWS from 'aws-sdk';
import * as uuidv4 from 'uuid/v4';

import { obtainSemaphore, countFreeSemaphores } from './leaderboards/services/semaphore/semaphore';
import { InputScoreUpdate, ScoreUpdate, LeaderboardRecord } from './leaderboards/model';
import { supportedIntervals, PIPELINE_UPDATE_CONCURRENCY } from './leaderboards/config';
import facetFactoryMethod from './leaderboards/services/facet-factory-method';
import { getScoreString, getDatedScoreBlockByScore } from './leaderboards/util';
import { retryPutUserScore, retryGetUserScore } from './leaderboards/repository/write-leaderboard';
import { leaderboardTableName, workerWriteSpeed, maxUpdateComplexity } from './leaderboards/services/semaphore/semaphore-model';

const queueUrl = 'https://sqs.us-east-1.amazonaws.com/145722906259/scoreQueue.fifo';

// TODO: Make sure processing time of lambda is about the same
const processingTimeInMilliseconds = 120 * 1000;

interface MessageWithScoreUpdate {
    message: AWS.SQS.Message
    scoreInputUpdate: InputScoreUpdate
}

interface MessageWithScoreUpdateAndComplexity extends MessageWithScoreUpdate {
    updateComplexity: number
}

const invokeNext = () => {
    const lambda = new AWS.Lambda();

    const params = {
        FunctionName: process.env.SELF_FUNCTION || 'Unknown', 
        InvocationType: 'Event', 
    };
    
    return lambda
        .invoke(params)
        .promise();
};

// Invokes more workers based on the count of free semaphores,
//  typically at minium it should invoke at least another to replace itself
const invokeMoreWorkers = async () => {
    const freeSemaphoreCount = await countFreeSemaphores();

    if (freeSemaphoreCount) {
        console.log(`Free semaphore count: ${freeSemaphoreCount}`);

        // Do not invoke more than 5 workers, trade off between slow rampup & invocation spikes
        //  due to read/write concurrency
        var invokeForNWorkers = _.clamp(freeSemaphoreCount, 1, 5);

        console.log(`Invoking ${invokeForNWorkers} more workers`);

        await BbPromise.all(_.times(invokeForNWorkers, invokeNext));

        console.log(`Finished invoking ${invokeForNWorkers} more workers`);
    }

    return;
}

const getMessages = async (size) => {
    const sqs = new AWS.SQS();

    const sqsDequeueParams = {
        QueueUrl: queueUrl,
        MaxNumberOfMessages: size,
    };

    const dequeueResponse = await sqs
        .receiveMessage(sqsDequeueParams)
        .promise();

    console.log(`DequeueResponse: ${JSON.stringify(dequeueResponse, null, 2)}`)

    return dequeueResponse.Messages || [];
}

const mapMessages = (sqsMessages: AWS.SQS.Message[]): MessageWithScoreUpdate[] => {
    const mappedMessages = _.map(
        sqsMessages,
        message => ({
            scoreInputUpdate: JSON.parse(message.Body || '{}') as InputScoreUpdate, 
            message
        })
    );

    console.log(`MappedMessages: ${JSON.stringify(mappedMessages, null, 2)}`)

    return mappedMessages;
}

const exploreScoreUpdate = (inputScoreUpdate: InputScoreUpdate) => {
    const { date, inputFacets, userId, score } = inputScoreUpdate;

    const inputFacetsWithDates = supportedIntervals
        .map(interval => ({ [interval]: date }));
    
    // Explode all of the scores
    const explodedFacets  = _.reduce(inputFacets, (acc, searchFacetValues, searchFacetKey) =>
        acc.flatMap(searchFacets => [null, ...searchFacetValues].map(
            facetValue => _.assign({}, searchFacets, { [searchFacetKey]: facetValue }))
        ),
        _(inputFacetsWithDates)
    )
    .value();

    // Map over userId and score specific stuff
    const scoreUpdates: ScoreUpdate[] = _.map(
        explodedFacets,
        facetKeyValueArray => ({
            userId,
            score,
            facets: _.map(facetKeyValueArray, (facetValue, facetKey) => facetFactoryMethod(facetKey, facetValue))
        })
    );

    return scoreUpdates;
}

const explodeScoreUpdates = (inputscoreUpdates: InputScoreUpdate[]) => {
    return inputscoreUpdates.map(exploreScoreUpdate)
}

export const compressScores = (scoreUpdates: ScoreUpdate[][]) => {
    const flatScoreUpdates = _.flatten(scoreUpdates);

    const groupedScoreUpdates = _.groupBy(
        flatScoreUpdates,
        scoreUpdate => `${scoreUpdate.userId}-${getScoreString(scoreUpdate.facets)}`
    );

    const compressedScoreUpdates = _.map(
        groupedScoreUpdates,
        similarScoreUpdateRecords => similarScoreUpdateRecords.reduce((acc, { score }) => 
            _.assign(acc, { score: acc.score + score })
        )
    );

    return compressedScoreUpdates;
};

const buildUpdates = (scoreUpdates: ScoreUpdate[]) => {
    return scoreUpdates.map(scoreUpdate => () => updateScore(scoreUpdate));
}

const updateScore = async (scoreUpdateRecord: ScoreUpdate) => {
    const { userId, score, facets } = scoreUpdateRecord;

    const scoreString = getScoreString(facets);

    // Reads the score so the value can be incremented
    const record = await retryGetUserScore(userId, scoreString);

    const currentScore = (record && record.score) || 0;

    const newScore = score + currentScore;

    const newRecord: LeaderboardRecord = {
        userId,
        score: newScore,
        datedScore: scoreString,
        datedScoreBlock: getDatedScoreBlockByScore(facets, newScore),
    };

    await retryPutUserScore(newRecord);
    
    return newRecord;
};

const pipelineUpdates = (updateTasks: (() => Promise<LeaderboardRecord>)[]) => {              
    return BbPromise.map(
        updateTasks,
        updateTask => updateTask(),
        { concurrency: PIPELINE_UPDATE_CONCURRENCY }
    );
}


const markCompleted = async (sqsMessages: AWS.SQS.Message[]) => {
    const sqs = new AWS.SQS();

    const entries = _.map(
        sqsMessages,
        message => 
            ({
                Id: uuidv4(),
                ReceiptHandle: message.ReceiptHandle,
            })
    );

    const sqsDeleteParams = {
        QueueUrl: queueUrl,
        Entries: entries,
    };

    const deleteResponse = await sqs
        .deleteMessageBatch(sqsDeleteParams as any)
        .promise();

    console.log(JSON.stringify({ deleteResponse }, null, 2));

    return;
};

const queryTableCapacity = async () => {
    const DynamoDB = new AWS.DynamoDB();

    const readResult = await DynamoDB
        .describeTable({ TableName: leaderboardTableName })
        .promise();

    console.log(JSON.stringify({ readResult }, null, 2))
        
    const capacity = Math.min(
        _.get(readResult, 'Table.ProvisionedThroughput.ReadCapacityUnits'),
        _.get(readResult, 'Table.ProvisionedThroughput.WriteCapacityUnits')
    );

    const numWriters = Math.ceil(capacity / workerWriteSpeed);

    const availableWriteUnits = Math.floor(capacity / numWriters);

    return availableWriteUnits;
};


// What happens if we cannot process 
const filterProcessibleMessages = async (inputScoreUpdates: MessageWithScoreUpdateAndComplexity[]) => {
    const updateCapacity = (await queryTableCapacity()) * (processingTimeInMilliseconds / 1000);

    console.log(JSON.stringify({ updateCapacity }));

    // We process the messages with smaller complexity first
    const orderedMessagesByUpdateWeighting = _.orderBy(
        inputScoreUpdates,
        'updateComplexity'
    );

    console.log(JSON.stringify({ orderedMessagesByUpdateWeighting }));

    const { processibleInputScoreUpdate, defferedInputScoreUpdate, remainingUpdateCapacity } = _.reduce(
        orderedMessagesByUpdateWeighting, 
        ({ remainingUpdateCapacity, processibleInputScoreUpdate, defferedInputScoreUpdate }, messageWithScoreUpdateAndComplexity) => ({
            defferedInputScoreUpdate: [
                ...defferedInputScoreUpdate,
                ...((messageWithScoreUpdateAndComplexity.updateComplexity > remainingUpdateCapacity) ? [messageWithScoreUpdateAndComplexity] : [])
            ],
            processibleInputScoreUpdate: [
                ...processibleInputScoreUpdate,
                ...((remainingUpdateCapacity > messageWithScoreUpdateAndComplexity.updateComplexity) ? [messageWithScoreUpdateAndComplexity] : [])
            ],
            remainingUpdateCapacity: remainingUpdateCapacity - (remainingUpdateCapacity > messageWithScoreUpdateAndComplexity.updateComplexity
                ? messageWithScoreUpdateAndComplexity.updateComplexity
                : 0
            ),
        }),
        {
            remainingUpdateCapacity: updateCapacity,
            processibleInputScoreUpdate: ([] as MessageWithScoreUpdateAndComplexity[]),
            defferedInputScoreUpdate: ([] as MessageWithScoreUpdateAndComplexity[])
        }
    );

    console.log(JSON.stringify({ processibleInputScoreUpdate, defferedInputScoreUpdate, remainingUpdateCapacity }));

    return { processibleInputScoreUpdate, defferedInputScoreUpdate };
}

const messageComplexity = (inputScoreUpdatesWithMessages: MessageWithScoreUpdate[]): MessageWithScoreUpdateAndComplexity[] => {
    return inputScoreUpdatesWithMessages.map(inputScoreUpdateWithMessage => 
        ({
            ...inputScoreUpdateWithMessage,
            updateComplexity: exploreScoreUpdate(inputScoreUpdateWithMessage.scoreInputUpdate).length,
        })
    );
}

const filterComplexMessages = (inputScoreUpdates: MessageWithScoreUpdateAndComplexity[]) => {
    const [processableMessages, complexMessages] = _.partition(
        inputScoreUpdates,
        ({ updateComplexity }) => (maxUpdateComplexity > updateComplexity)
    );

    return { processableMessages, complexMessages };
}

// --

const reduceMessages = async (sqsMessages: AWS.SQS.Message[]) => {
    const inputScoreUpdatesWithMessages = _.flow([mapMessages, messageComplexity])(sqsMessages);

    const { processableMessages, complexMessages } = filterComplexMessages(inputScoreUpdatesWithMessages);

    console.log(
        `Processible Messages: ${JSON.stringify({ length: processableMessages.length, processableMessages })}`,
        `Complex Messages: ${JSON.stringify({ length: complexMessages.length, complexMessages })}`
    );

    // Delete the messages we cannot process because they are too complex
    if (complexMessages.length) {
        await markCompleted(_.map(complexMessages, 'message'));
    }

    const { processibleInputScoreUpdate, defferedInputScoreUpdate } = await filterProcessibleMessages(processableMessages);

    console.log(`Processible Messages: ${processibleInputScoreUpdate.length}, Deffered Messages: ${defferedInputScoreUpdate.length}`);

    return processibleInputScoreUpdate;
}

const processMessages = async (processibleInputScoreUpdate: MessageWithScoreUpdateAndComplexity[]) => {
    const inputScoreUpdates = _.map(processibleInputScoreUpdate, 'scoreInputUpdate') as InputScoreUpdate[];

    const userIdsMessagesPerUserId = _.countBy(inputScoreUpdates, 'userId');

    console.log('Messages Per UserId: ', JSON.stringify(userIdsMessagesPerUserId, null, 2));

    const explodedScoreUpdates = explodeScoreUpdates(inputScoreUpdates);

    console.log(`Exploded Score Updates: ${_.reduce(explodedScoreUpdates, (acc, scoreUpdates) => acc + scoreUpdates.length, 0)}`);

    const compressedScores = compressScores(explodedScoreUpdates);

    console.log(`Compressed Score Updates: ${compressedScores.length}`);

    const scoreUpdates = buildUpdates(compressedScores);

    console.log(`Generated Database Writes: ${scoreUpdates.length}`);

    const result = await pipelineUpdates(scoreUpdates);

    // TODO: Only mark the messages which have actually been complete, completed
    if (processibleInputScoreUpdate.length) {
        await markCompleted(_.map(processibleInputScoreUpdate, 'message'));
    }

    return result;
}

/*
    Problem:
        - What happens when I get messages which have really low write requirements?
            -> Might spend a lot of time polling the queue, and little time writting
        - How 
*/
export const handler = async (event, context, cb) => {
    return BbPromise.using(obtainSemaphore(), async () => {
        try {
            while (context.getRemainingTimeInMillis() > 240 * 1000) {
                const now = Date.now()

                console.log('-----------------------------------------------------------------')
                console.log('Getting Messages');

                const sqsMessages = await getMessages(10);

                console.log(`Fetched Messages: ${sqsMessages.length}`);

                // We got not more messages, let's not invoke more workers
                if (sqsMessages.length === 0) {
                    return false;
                }

                const processableMessages = await reduceMessages(sqsMessages);

                const result = await processMessages(processableMessages);

                console.log(`Finished Database Writes: ${result.length}`);
                console.log('-----------------------------------------------------------------')
                console.log(`--- Writes p/s ${result.length / ((Date.now() - now) / 1000)} @ concurrency level: ${PIPELINE_UPDATE_CONCURRENCY}`)
                console.log('-----------------------------------------------------------------')
            }

            // We have successfully processed everything that we could and we ran out of time
            return true;
        } catch (err) {
            return console.error(err.message, err.stack) || false;
        }
    })
    .then(moreWorkers => moreWorkers
        ? (console.log('Probably more workers, invoking more workers') || invokeMoreWorkers())
        : console.log('Finished processing, not invoking any more workers')
    )
    .finally(() => console.log('Finished') || cb(undefined))
};
