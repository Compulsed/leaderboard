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

import {
    getDatedScore,
    getDatedScoreBlockByScore,
} from '../util';

const tableName = process.env.LEADERBOARD_TABLE || 'Unknown';
const docClient = new AWS.DynamoDB.DocumentClient();


/*
    Given a userId and the dateScore eg (month_2017/09_1), it will return 
        all the records. You can think of this as 'Given a list of scoreboards, 
        give me the scores of for the user'

    You might do this so that you know what records to update when you are incrementing
        their scores

    Note: Returns null if they are not present on that board
*/
interface UserScoreDict {
    [timeInterval: string]: LeaderboardRecord | null;
}

interface UserScoreLookupDict {
    [timeInterval: string]: string
}

// TODO: Consider batchGetItem
export const getScoresById = async (userId: string, datedScores: UserScoreLookupDict): Promise<UserScoreDict> => {
    const usersScoresPromises = _.mapValues(
        datedScores,
        datedScore => getUserScore(userId, datedScore)
    );

    return (await BbPromise.props(usersScoresPromises)) as UserScoreDict;
}

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
        IndexName: 'scoresByDatedScoreBlock',        
        KeyConditionExpression: 'datedScoreBlock = :hkey',
        ExpressionAttributeValues: {
            ':hkey': datedScoreBlock,
        },
    };
    
    // TODO: Consider size limit of 1MB & Paging
    const readResult = await docClient
        .query(params)
        .promise();

    return (readResult.Items || []) as LeaderboardRecord[];
};

export const updateScore = async (userId: string, timeInterval: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData, scoreIncrement: number) => {
    const datedScore = getDatedScore(timeInterval, date, scoreFacet, scoreFacetsData);

    let createdRecord: (LeaderboardRecord | null) = null;

    const options = {
        randomize: true, 
        minTimeoutBeforeFirstRetry: 1, 
        maxTimeoutBetweenRetries: 1000
    };

    await promiseRetry(async (retry, number) => {
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
            TableName: tableName,  
            Item: newRecord,
            ConditionExpression: '#score = :currentScore or attribute_not_exists(#score)',
            ExpressionAttributeNames: { '#score': 'score' },
            ExpressionAttributeValues: { ':currentScore': score }
        };

        try {
            await docClient
                .put(putParams)
                .promise();
        } catch (err) {
            console.log('Error: ', JSON.stringify({ err }));

            // NOTE: Should not get ConditionalCheckFailedException with batching
            if (err.code === 'ProvisionedThroughputExceededException' || err.code === 'ConditionalCheckFailedException') {
                console.log('Retrying');
                
                retry(err);
            }
     
            throw err;
        }

        createdRecord = newRecord;                
    }, options);

    return createdRecord;
};
