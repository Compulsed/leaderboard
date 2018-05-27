import * as AWS from 'aws-sdk';
import * as promiseRetry from 'promise-retry';
import * as _ from 'lodash';
import * as DataLoader from 'dataloader';
import * as BbPromise from 'bluebird';

import { LeaderboardRecord } from '../model';

const tableName = process.env.LEADERBOARD_TABLE || 'Unknown';
const indexName = process.env.SCORES_BY_DATED_SCORE_BLOCK_INDEX || 'Unknown';

const DynamoDBService = new AWS.DynamoDB({ 
    httpOptions: {
        timeout: 2000, // 120000 - default timeout
    },
})

const docClient = new AWS.DynamoDB.DocumentClient({ service: DynamoDBService });

const batchWrite = async (leaderboardRecords: LeaderboardRecord[]) => {
    let unprocessedRecords = _.map(leaderboardRecords, leaderboardRecord =>
        ({
            PutRequest: {
                Item: leaderboardRecord,
            },
        })
    );

    while (unprocessedRecords.length) {
        const params = {
            RequestItems: { [tableName]: unprocessedRecords },
        };
    
        try {
            const response = await docClient
                .batchWrite(params)
                .promise();

            unprocessedRecords = (response.UnprocessedItems || []) as typeof unprocessedRecords;
        } catch (err) {
            if (err.code !== 'ProvisionedThroughputExceededException') {                
                throw err;
            }
        }
            
        // If there was unprocessed records, let's backoff
        if (unprocessedRecords.length) {
            await BbPromise.delay(1000);
        }
    }

    return _.times(leaderboardRecords.length, _.constant(null));
};

const batchRead = async (leaderboardRecords: { userId: string, datedScore: string }[]) => {
    let readRequests = _.map(leaderboardRecords, leaderboardRecord =>
        ({
            userId: leaderboardRecord.userId,
            datedScore: leaderboardRecord.datedScore
        })
    );

    let results: LeaderboardRecord[] = [];

    while (readRequests.length) {
        const params = {
            RequestItems: { 
                [tableName]: {
                    Keys: readRequests,
                },
            },
        };
    
        try {
            const response = await docClient
                .batchGet(params)
                .promise();

            readRequests = (response.UnprocessedKeys || []) as typeof readRequests;
            results = [...results, ...((response.Responses || []) as LeaderboardRecord[])];
        } catch (err) {
            if (err.code !== 'ProvisionedThroughputExceededException') {                
                throw err;
            }
        }
            
        // If there was unprocessed records, let's backoff
        if (readRequests.length) {
            await BbPromise.delay(1000);
        }
    }

    const resultsByIndex = _.keyBy(
        results,
        ({userId, datedScore}) => `${userId}-${datedScore}`
    );

    return _.map(
        leaderboardRecords,
        ({userId, datedScore}) => resultsByIndex[`${userId}-${datedScore}`] || null
    );
};

const writeLoader = new DataLoader(
    batchWrite,
    { cache: false, maxBatchSize: 25 }
);

const readLoader = new DataLoader(
    batchRead,
    { cache: false, maxBatchSize: 25 }
);

export const retryPutUserScore = (leaderboardRecord: LeaderboardRecord) => {
    return writeLoader.load(leaderboardRecord);
};

export const getUserScore = (userId: string, datedScore: string) => {
    return readLoader.load({ userId, datedScore });
};

const promiseRetryOptions = {
    randomize: true, 
    retries: 10 * 1000, // Should be high enough
    minTimeoutBeforeFirstRetry: 1000,
    maxTimeoutBetweenRetries: 16 * 1000, // 16 seconds
};

const putUserScore = (leaderboardRecord: LeaderboardRecord) => {
    const putParams = {
        TableName: tableName,
        Item: leaderboardRecord,
    };

     return docClient
        .put(putParams)
        .promise()
};

export const retryPutUserScoreSingle = (leaderboardRecord: LeaderboardRecord) => {
    return promiseRetry(async (retry, number) => {
        return putUserScore(leaderboardRecord).catch(err => {
            if (err.code === 'ProvisionedThroughputExceededException') {                
                retry(err);
            }

            throw err;
        });
    }, promiseRetryOptions);
}


const getUserScoreSingle = async (userId: string, datedScore: string) => {
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