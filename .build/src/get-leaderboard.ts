import 'source-map-support/register'
import * as uuid from 'uuid/v4';
import { Context, Callback } from 'aws-lambda';
import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

import { NO_TAG_VALUE, ScoreFacet, ScoreFacetTuple, TimeInterval } from './leaderboards/model';

import { getTop } from './leaderboards/services/read-leaderboard';

const CORS_HEADERS = {
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
    'Access-Control-Allow-Methods': 'OPTIONS,POST',
    'Access-Control-Allow-Origin': '*',
};

export const handler = async (event: any, context: Context, cb: Callback) => {
    console.log('event', JSON.stringify({ event }, null, 2));

    try {
        const interval: TimeInterval = _.get(event, 'queryStringParameters.interval', TimeInterval.ALL_TIME);
        const tag: string = _.get(event, 'queryStringParameters.tag', NO_TAG_VALUE);

        let scoreFacet:ScoreFacetTuple = [ScoreFacet.ALL, undefined];
    
        if (event.queryStringParameters.location) {
            scoreFacet = [ScoreFacet.LOCATION, event.queryStringParameters.location];
        }
    
        if (event.queryStringParameters.organisationId) {
            scoreFacet = [ScoreFacet.ORGANISATION, event.queryStringParameters.organisationId];
        }

        const scores = await getTop(
            interval,
            new Date(),
            scoreFacet[0],
            scoreFacet[1],
            tag,
            50
        );

        const scoresWithPosition = scores
            .map((score, index) => Object.assign({}, score, { index }))

        cb(null, {
            statusCode: 500,
            headers: CORS_HEADERS,
            body: JSON.stringify({ scoresWithPosition }, null, 2),
        });
    } catch (err) {
        return console.error(err.message, err.stack) || cb(err);
    }
};
