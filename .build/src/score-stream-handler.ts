import 'source-map-support/register'
import * as uuid from 'uuid/v4';
import { Context, Callback } from 'aws-lambda';
import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

import { ScoreFacet, ScoreFacetTuple } from './leaderboards/model';
import * as leaderboardService from './leaderboards/services/leaderboard';

// Defines out many sets of score record writes can be in flight at one time
//  scoreRecordWritesInFlight = (No. TimeIntervals) * (No. Facets) * (recordConcurrencyLevel)
const recordConcurrencyLevel = 1;

interface ScoreRecord {
    userId: string
    score: number
    organisationId?: string
    location?: string
}

const processRecord = async (scoreRecord: ScoreRecord) => {
    try {
        const facets:ScoreFacetTuple[] = [
            [ScoreFacet.ALL, undefined]
        ];

        if (scoreRecord.location) {
            facets.push([ScoreFacet.LOCATION, scoreRecord.location]);
        }

        if (scoreRecord.organisationId) {
            facets.push([ScoreFacet.ORGANISATION, scoreRecord.organisationId])
        }

        await leaderboardService.updateScore(
            scoreRecord.userId,
            new Date(),
            facets,
            scoreRecord.score
        );
    } catch (err) {
        console.error(err, err.stack);
    }

    return scoreRecord;
}

const aggregateScores = (records: ScoreRecord[]) =>
    _(records)
        .groupBy('userId')
        .map(scores => scores.reduce((acc, { score }) => 
            Object.assign(acc, { score: acc.score + score }))
        )
        .value()

const formatKinesisRecords = (kinesisRecords) => {
    return kinesisRecords
        .map(record => Buffer.from(record.kinesis.data, 'base64').toString())
        .map(recordJSONString => JSON.parse(recordJSONString))
        .map(_.property('data'));
}

/*
    Batch Size: Defined in Lambda Function
    Write Calculation:
        -> TimeIntervals * Facets * Records 
    
    TimeIntervals: (Second <-> All Time)    = 8
    Facets: { All, Organisation, Location } = 3
    Number of records (BatchSize)           = 10
        -> 8 * 3 * 10
        -> 240

    5 WCU
        -> 48 seconds. max per batch
*/
export const handler = async (event: any, context: Context, cb: Callback) => {
    console.log('event', JSON.stringify({ event }, null, 2));

    try {
        const formattedRecords = formatKinesisRecords(event.Records);

        console.log(JSON.stringify({ formattedRecords }, null, 2));

        const aggregatedScores = aggregateScores(formattedRecords);

        console.log(JSON.stringify({ aggregatedScores }, null, 2));

        const processedRecords = await BbPromise.map(
            aggregatedScores,
            processRecord,
            { concurrency: recordConcurrencyLevel }
        );

        console.log(JSON.stringify({ processedRecords }, null, 2));

        return cb(undefined, { message: 'Success' });
    } catch (err) {
        return console.error(err.message, err.stack) || cb(err);
    }
};
