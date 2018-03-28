import * as AWS from 'aws-sdk';
import * as promiseRetry from 'promise-retry';

import { LeaderboardRecord } from '../model';

const tableName = process.env.LEADERBOARD_TABLE || 'Unknown';
const indexName = process.env.SCORES_BY_DATED_SCORE_BLOCK_INDEX || 'Unknown';

const docClient = new AWS.DynamoDB.DocumentClient();

const promiseRetryOptions = {
    randomize: true, 
    retries: 10 * 1000, // Should be high enough
    minTimeoutBeforeFirstRetry: 10,
    maxTimeoutBetweenRetries: 1000,
};

const putUserScore = (leaderboardRecord: LeaderboardRecord) => {
    const putParams = {
        TableName: tableName,
        Item: leaderboardRecord,
    };

     return docClient
        .put(putParams)
        .promise()
}

export const retryPutUserScore = (leaderboardRecord: LeaderboardRecord) => {
    return promiseRetry(async (retry, number) => {
        return putUserScore(leaderboardRecord).catch(err => {
            if (err.code === 'ProvisionedThroughputExceededException') {                
                retry(err);
            }

            throw err;
        });
    }, promiseRetryOptions);
}

const getUserScore = async (userId: string, datedScore: string) => {
    const params = {
        TableName: tableName,
        Key: { userId, datedScore },
        ConsistentRead: true,
    };

    const getResult = await docClient
        .get(params)
        .promise();

    return (getResult.Item || null) as (LeaderboardRecord | null);
}

export const retryGetUserScore = (userId: string, scoreString: string) => {
    return promiseRetry(async (retry, number) => {
        return getUserScore(userId, scoreString).catch(err => {
            if (err.code === 'ProvisionedThroughputExceededException') {                
                retry(err);
            }

            throw err;
        });
    }, promiseRetryOptions);
}