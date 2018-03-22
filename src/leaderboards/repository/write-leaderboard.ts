import * as AWS from 'aws-sdk';
import * as BbPromise from 'bluebird';
import * as _ from 'lodash';
import * as promiseRetry from 'promise-retry';

// Model
import { LeaderboardRecord, TimeInterval } from '../model';
import { PartialSearchFacets } from '../util';

// Functions
import { getDatedScore, getDatedScoreBlockByScore } from '../util';
import { getUserScore } from './read-leaderboard';

const docClient = new AWS.DynamoDB.DocumentClient();

const promiseRetryOptions = {
    randomize: true, 
    retries: 10 * 1000, // Should be high enough
    minTimeoutBeforeFirstRetry: 10,
    maxTimeoutBetweenRetries: 1000,
};

export interface ScoreUpdateRecord {
    userId: string
    score: number
    date: Date
    timeInterval: TimeInterval
    facets: PartialSearchFacets
}

export const updateScore = async (scoreUpdateRecord: ScoreUpdateRecord) => {
    const { userId, score, date, timeInterval, facets } = scoreUpdateRecord;

    const datedScore = getDatedScore(timeInterval, date, facets);

    // Reads the score so the value can be incremented
    const record = await promiseRetry(async (retry, number) => {
        return getUserScore(userId, datedScore).catch(err => {
            if (err.code === 'ProvisionedThroughputExceededException') {                
                retry(err);
            }

            throw err;
        });
    }, promiseRetryOptions);

    const currentScore = (record && record.score) || 0;

    const newScore = score + currentScore;

    const newRecord: LeaderboardRecord = {
        userId,
        score: newScore,
        datedScore, 
        datedScoreBlock: getDatedScoreBlockByScore(timeInterval, date, facets, score),
    };

    const putParams = {
        TableName: process.env.LEADERBOARD_TABLE as string,
        Item: newRecord,
    };

    await promiseRetry(async (retry, number) => {
        return docClient.put(putParams).promise().catch(err => {
            if (err.code === 'ProvisionedThroughputExceededException') {                
                retry(err);
            }

            throw err;
        });
    }, promiseRetryOptions);

    return newRecord;
};
