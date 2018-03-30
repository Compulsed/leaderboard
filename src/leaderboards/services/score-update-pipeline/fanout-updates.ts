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

    return BbPromise.all(scoreUpdatePromises)
        .then(leaderboardRecord => _.flatten(leaderboardRecord));
}

const invokeWithScoreUpdates = (scoreUpdates: ScoreUpdate[]): Promise<LeaderboardRecord[]> => {
    const params = {
        FunctionName: updateFunctionName, 
        InvocationType: 'RequestResponse', 
        Payload: JSON.stringify(scoreUpdates)
    };

    return lambda.invoke(params)
        .promise()
        .then(result => JSON.parse(result.Payload as string))
}

export default fanoutUpdates;