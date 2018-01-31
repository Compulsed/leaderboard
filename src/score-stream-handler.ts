import 'source-map-support/register'
import * as uuid from 'uuid/v4';
import { Context, Callback } from 'aws-lambda';
import * as BbPromise from 'bluebird';

import { ScoreFacet, ScoreFacetTuple } from './leaderboards/model';
import * as leaderboardService from './leaderboards/services/leaderboard';

const processRecord = async (record) => {
    // TODO: Retry strategy
    try {
        const facets:ScoreFacetTuple[] = [
            [ScoreFacet.ALL, undefined]
        ];

        if (record.data.location) {
            facets.push([ScoreFacet.LOCATION, record.data.location]);
        }

        if (record.data.organisationId) {
            facets.push([ScoreFacet.ORGANISATION, record.data.organisationId])
        }

        await leaderboardService.updateScore(
            record.data.userId,
            new Date(),
            facets,
            record.data.score
        );
    } catch (err) {
        console.error(err, err.stack);
    }

    return;
}

const processRecords = (records) => {
    return BbPromise.map(
        records,
        processRecord,
        { concurrency: 2 }
    );
}

const formatKinesisRecords = (kinesisRecords) => {
    return kinesisRecords
        .map(record => Buffer.from(record.kinesis.data, 'base64').toString())
        .map(recordJSONString => JSON.parse(recordJSONString));
}

export const handler = async (event: any, context: Context, cb: Callback) => {
    console.log('event', JSON.stringify({ event }, null, 2));

    try {
        const formattedRecords = formatKinesisRecords(event.Records);

        await processRecords(formattedRecords);

        return cb(undefined, { message: 'Success' });
    } catch (err) {
        return console.error(err.message, err.stack) || cb(err);
    }
};
