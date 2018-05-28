import 'source-map-support/register'
import * as AWS from 'aws-sdk';

import { LeaderboardRecord } from './leaderboards/model';

// TODO: Export
const leaderboardTableName = process.env.LEADERBOARD_TABLE || 'Unknown';
const leaderboardIndexName = process.env.SCORES_BY_DATED_SCORE_BLOCK_INDEX || 'Unknown';

const fakeRecord: LeaderboardRecord = {
    userId: String(Math.floor(Math.random() * 10000)),
    score: 123,
    datedScore: '123',
    datedScoreBlock: '123', 
};

// TODO: Extend to GSI
export const handler = async (event, context, cb) => {
    try {
        const docClient = new AWS.DynamoDB.DocumentClient();
    
        const writeResult = await docClient
            .put({ 
                TableName: leaderboardTableName,
                Item: fakeRecord
            })
            .promise();

        console.log(JSON.stringify({ writeResult }));

        const readResult = await docClient
            .get({ 
                TableName: leaderboardTableName,
                Key: {
                    userId: fakeRecord.userId,
                    datedScore: fakeRecord.datedScore,
                },
            })
            .promise();

        console.log(JSON.stringify({ readResult }));

        const indexReadResult = await docClient
            .query({
                TableName: leaderboardTableName,
                IndexName: leaderboardIndexName,
                KeyConditionExpression: 'datedScoreBlock = :hkey',
                ExpressionAttributeValues: {
                    ':hkey': fakeRecord.datedScoreBlock,
                },
            })
            .promise();

        console.log(JSON.stringify({ indexReadResult }));
            
        cb(undefined, { message: 'Success!' });
    } catch (err) {
        console.error(err) || cb(err);
    }
}