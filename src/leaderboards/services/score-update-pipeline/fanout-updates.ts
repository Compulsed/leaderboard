import * as moment from 'moment';
import * as _ from 'lodash';
import * as uuid from 'uuid/v4';
import * as BbPromise from 'bluebird';
import * as AWS from 'aws-sdk';

// Model
import { ScoreUpdate, LeaderboardRecord} from '../../model';

// Config
import { LAMBDA_CHUNCK_SIZE } from '../../config';

// Setup
const lambda = new AWS.Lambda();
const updateFunctionName = process.env.UPDATE_FUNCTION || 'unknown';

// Export
const fanoutUpdates = (scoreUpdates: ScoreUpdate[]) => {
    const chunkedScores = _.chunk(scoreUpdates, LAMBDA_CHUNCK_SIZE)

    const scoreUpdatePromises = chunkedScores
        .map(invokeWithScoreUpdates);

    // Logging
    scoreUpdatePromises
        .map(promise => promise
            .then(records => console.log(`Done ${records && records.length} records`))
        );

    return BbPromise.all(scoreUpdatePromises)
        .then(leaderboardRecord => _.flatten(leaderboardRecord));
}

const timeoutHanlder = err => {
    if (err instanceof BbPromise.TimeoutError) {
        console.log('Timed out lambda call');
    }

    return [];
}

const invokeWithScoreUpdates = (scoreUpdates: ScoreUpdate[]): Promise<LeaderboardRecord[]> => {
    const params = {
        FunctionName: updateFunctionName, 
        InvocationType: 'RequestResponse', 
        Payload: JSON.stringify(scoreUpdates)
    };

    const lambdaPromise = lambda.invoke(params)
        .promise()

    const resultPromise = BbPromise.resolve(lambdaPromise)
        .timeout(10 * 1000)
        .then(result => JSON.parse(result.Payload as string))
        .catch(timeoutHanlder);

    return resultPromise as Bluebird<LeaderboardRecord>[];
}

export default fanoutUpdates;