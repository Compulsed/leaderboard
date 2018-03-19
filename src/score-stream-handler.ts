import 'source-map-support/register'
import * as uuid from 'uuid/v4';
import { Context, Callback } from 'aws-lambda';
import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

import { NO_TAG_VALUE, ScoreFacet, ScoreFacetTuple, TimeInterval } from './leaderboards/model';
import { getScoreUpdates } from './leaderboards/services/write-leaderboard';

// Defines the amount of update tasks that can be running at one time,
//  an update task involves (1 Read & 1 Write)
const recordConcurrencyLevel = 10;

const timeIntervals = [
    TimeInterval.HOUR,
    TimeInterval.DAY,
    TimeInterval.WEEK,
    TimeInterval.MONTH,
    TimeInterval.YEAR,
    TimeInterval.ALL_TIME,
];

interface ScoreRecord {
    userId: string
    score: number
    organisationId?: string
    location?: string
    tags?: string[]
}

const processRecord = (scoreRecord: ScoreRecord) => {
    const facets:ScoreFacetTuple[] = [
        [ScoreFacet.ALL, undefined]
    ];

    if (scoreRecord.location) {
        facets.push([ScoreFacet.LOCATION, scoreRecord.location]);
    }

    if (scoreRecord.organisationId) {
        facets.push([ScoreFacet.ORGANISATION, scoreRecord.organisationId])
    }

    const tags = [NO_TAG_VALUE].concat(scoreRecord.tags || []);

    const scoreUpdates = getScoreUpdates(
        scoreRecord.userId,
        new Date(),
        timeIntervals,
        facets,
        tags,
        scoreRecord.score
    );

    return scoreUpdates;
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

// Total Batch Duration
//  -> ((Tags) * (Facets) * (TimeIntervals) * (Batch Size) / MIN(RCU, WCU))
//  -> ((3 * 2 * 6 * 10) / 5)
//  -> 72 seconds
export const handler = async (event: any, context: Context, cb: Callback) => {
    console.log('event', JSON.stringify({ event }, null, 2));

    try {
        const formattedRecords = formatKinesisRecords(event.Records);

        console.log(JSON.stringify({ formattedRecords }, null, 2));

        const aggregatedScores = aggregateScores(formattedRecords);

        console.log(JSON.stringify({ aggregatedScores }, null, 2));

        const scoreUpates = _(aggregatedScores)
            .map(processRecord)
            .flatten()
            .value();
        
        const processedRecords = await BbPromise.map(
            scoreUpates,
            scoreUpdate => scoreUpdate(),
            { concurrency: recordConcurrencyLevel }
        );

        console.log(JSON.stringify({ processedRecords }, null, 2));

        console.log(JSON.stringify({
            fomattedRecords: formattedRecords.length,
            scoreAggregationCount: aggregatedScores.length,
            scoresWritten: processedRecords.length,
        }, null, 2));

        return cb(undefined, { message: 'Success' });
    } catch (err) {
        return console.error(err.message, err.stack) || cb(err);
    }
};
