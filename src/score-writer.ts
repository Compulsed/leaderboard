import 'source-map-support/register'

import * as BbPromise from 'bluebird';
import * as _ from 'lodash';
import * as AWS from 'aws-sdk';

import { obtainSemaphore, countFreeSemaphores } from './leaderboards/services/semaphore/semaphore';
import { InputScoreUpdate, ScoreUpdate, LeaderboardRecord } from './leaderboards/model';
import { supportedIntervals, PIPELINE_UPDATE_CONCURRENCY } from './leaderboards/config';
import facetFactoryMethod from './leaderboards/services/facet-factory-method';
import { getScoreString, getDatedScoreBlockByScore } from './leaderboards/util';
import { retryPutUserScore, retryGetUserScore } from './leaderboards/repository/write-leaderboard';

const queueUrl = 'https://sqs.us-east-1.amazonaws.com/145722906259/scoreQueue.fifo';

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
        console.log(`Invoking ${freeSemaphoreCount} more workers`);

        await BbPromise.all(_.times(freeSemaphoreCount, invokeNext));

        console.log(`Finished invoking ${freeSemaphoreCount} more workers`);
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

    const mappedMessages: InputScoreUpdate[] = _.map(
        dequeueResponse.Messages || [],
        message => JSON.parse(message.Body || '{}') // TODO: handle missing body
    );

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

const compressScores = (scoreUpdates: ScoreUpdate[][]) => {
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

/*
    Problem:
        - What happens when I get messages which have really low write requirements?
            -> Might spend a lot of time polling the queue, and little time writting
        - Need to make the messages as successful
*/
export const handler = async (event, context, cb) => {
    return BbPromise.using(obtainSemaphore(), async () => {
        while (context.getRemainingTimeInMillis() > 240 * 1000) {
            const now = Date.now()

            console.log('-----------------------------------------------------------------')
            console.log('Getting Messages');

            const inputScoreUpdate = await getMessages(10);

            console.log(`Fetched Messages: ${inputScoreUpdate.length}`);

            // We got not more messages, let's not invoke more workers
            if (inputScoreUpdate.length === 0) {
                return false;
            }

            const explodedScoreUpdates = explodeScoreUpdates(inputScoreUpdate);
    
            console.log(`Exploded Score Updates: ${explodedScoreUpdates.length}`);

            const compressedScores = compressScores(explodedScoreUpdates);
    
            console.log(`Compressed Score Updates: ${compressedScores.length}`);

            const scoreUpdates = buildUpdates(compressedScores);
    
            console.log(`Generated Database Writes: ${scoreUpdates.length}`);

            const result = await pipelineUpdates(scoreUpdates);    

            console.log(`Finished Database Writes: ${result.length}`);
            console.log('-----------------------------------------------------------------')
            console.log(`--- Writes p/s ${compressedScores.length / ((Date.now() - now) / 1000)} @ concurrency level: ${PIPELINE_UPDATE_CONCURRENCY}`)
            console.log('-----------------------------------------------------------------')

        }

        // We have successfully processed everything that we could and we ran out of time
        return true;
    })
    .then(moreWorkers => moreWorkers
        ? (console.log('Probably more worker, invoking more workers') || invokeMoreWorkers())
        : console.log('Finished processing, not invoking any more workers')
    )
    .finally(() => console.log('Finished') || cb(undefined))
};
