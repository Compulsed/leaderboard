import * as AWS from 'aws-sdk';
import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

import {  LeaderboardRecord } from '../model';

const tableName = process.env.LEADERBOARD_TABLE || 'Unknown';
const indexName = process.env.SCORES_BY_DATED_SCORE_BLOCK_INDEX || 'Unknown';

const DynamoDBService = new AWS.DynamoDB({ 
    httpOptions: {
        timeout: 2000, // 120000 - default timeout
    },
})

const docClient = new AWS.DynamoDB.DocumentClient({ service: DynamoDBService });

// Gets _ALL_ the scores that belong a to a user
export const getUserScore = async (userId: string, datedScore: string) => {
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

/*
    Return every score in the score block, input eg. month_2017/09_1
*/
export const getScoresInScoreBlock = async (datedScoreBlock: string): Promise<LeaderboardRecord[]> => {
    const params = {
        TableName: tableName,
        IndexName: indexName,
        KeyConditionExpression: 'datedScoreBlock = :hkey',
        ExpressionAttributeValues: {
            ':hkey': datedScoreBlock,
        },
    };
    
    console.log(JSON.stringify({ params }));

    // TODO: Consider size limit of 1MB & Paging
    const readResult = await docClient
        .query(params)
        .promise();

    console.log(JSON.stringify({ readResult }));

    return (readResult.Items || []) as LeaderboardRecord[];
};
