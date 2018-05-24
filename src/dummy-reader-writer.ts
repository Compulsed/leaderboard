import 'source-map-support/register'
import * as AWS from 'aws-sdk';

import { leaderboardTableName, } from './leaderboards/services/semaphore/semaphore-model';
import { LeaderboardRecord } from './leaderboards/model';

const fakeRecord: LeaderboardRecord = {
    userId: '123',
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
                    datedScore: fakeRecord.datedScore
                },
            })
            .promise();

        console.log(JSON.stringify({ readResult }));
        
        cb(undefined, { message: 'Success!' });
    } catch (err) {
        console.error(err) || cb(err);
    }
}