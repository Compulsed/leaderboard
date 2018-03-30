import 'source-map-support/register'
import * as uuid from 'uuid/v4';
import { Context, Callback } from 'aws-lambda';
import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

import { getScores } from './leaderboards/services/read-leaderboard';

const CORS_HEADERS = {
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
    'Access-Control-Allow-Methods': 'OPTIONS,POST',
    'Access-Control-Allow-Origin': '*',
};

export const handler = async (event: any, context: Context, cb: Callback) => {
    console.log('event', JSON.stringify({ event }, null, 2));

    try {
        const inputScoreFacets = _.get(event, 'queryStringParameters', {});

        const timeInterval = 'day';
        const date = Date.now();

        const scores = await getScores(timeInterval, date, inputScoreFacets, 50);

        const scoresWithPosition = scores
            .map((score, index) => Object.assign({}, score, { index }))

        cb(undefined, {
            statusCode: 200,
            headers: CORS_HEADERS,
            body: JSON.stringify({ scoresWithPosition }, null, 2),
        });
    } catch (err) {
        return console.error(err.message, err.stack) || cb(err);
    }
};
