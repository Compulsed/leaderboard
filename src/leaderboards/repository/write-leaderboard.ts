import * as AWS from 'aws-sdk';
import * as BbPromise from 'bluebird';
import * as _ from 'lodash';
import * as promiseRetry from 'promise-retry';

import { 
    LeaderboardRecord,
    TimeInterval,
    ScoreFacet,
    ScoreFacetData
} from '../model';

import { getDatedScore, getDatedScoreBlockByScore } from '../util';

import { getUserScore } from './read-leaderboard';

const docClient = new AWS.DynamoDB.DocumentClient();

const promiseRetryOptions = {
    randomize: true, 
    retries: 10 * 1000, // Should be high enough
    minTimeoutBeforeFirstRetry: 10,
    maxTimeoutBetweenRetries: 1000,
};

export const updateScore = async (userId: string, timeInterval: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData, scoreIncrement: number) => {
    const datedScore = getDatedScore(timeInterval, date, scoreFacet, scoreFacetsData);

    // Reads the score so the value can be incremented
    const record = await getUserScore(userId, datedScore);

    const score = (record && record.score) || 0;

    const newScore = score + scoreIncrement;

    const newRecord: LeaderboardRecord = {
        userId,
        score: newScore,
        datedScore, 
        datedScoreBlock: getDatedScoreBlockByScore(
            timeInterval,
            date,
            scoreFacet,
            scoreFacetsData,
            newScore
        ),
    };

    const putParams = {
        TableName: process.env.LEADERBOARD_TABLE,  
        Item: newRecord,
    };

    const updatedRow = await promiseRetry(async (retry, number) => {
        return docClient.put(putParams).promise().catch(err => {
            if (err.code === 'ProvisionedThroughputExceededException') {                
                retry(err);
            }

            throw err;
        });
    }, promiseRetryOptions);

    return updatedRow;
};
