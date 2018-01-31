import * as AWS from 'aws-sdk';
import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

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

    while (true) {
        try {
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
                ExpressionAttributeNames: {
                    '#score' : 'score'
                },
                ExpressionAttributeValues: {
                    ':currentScore' : score,
                }
            };

            // This considers what happens when another concurrent writter
            //  tries to update the record. I do not think atomic increment
            //  would work correct because we may change the `datedScoreBlock` index
            //  Note:
            //   Always
            //      - 2 RCU against base table (consistent read)
            //   If Successful
            //      - 1 WCU on the base table (always)
            //      - If first score of the day, 1 WCU against the index, else 2 WRU
            //  TODO:
            //   - Consider a better backoff, what makes sense on our dev machines
            //      may not make sense in product & vice versa
            //   - Think about read/write backoffs for things like the limits of the
            //      table throughput, these errors should probably be handled differently
            //   - Consider a batchPut or an update & what makes more sense + why.
            await docClient
                .put(putParams)
                .promise();

            createdRecord = newRecord;                
        } catch (err) {
            if (err.code === 'ProvisionedThroughputExceededException') {
                await BbPromise.delay(
                    Math.round(Math.random() * 2000)
                );
    
                continue;
            }

            return Promise.reject(err);
        }

        break;
    }

    return createdRecord;
};
