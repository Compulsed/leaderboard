import * as moment from 'moment';
import * as _ from 'lodash';
import * as uuid from 'uuid/v4';
import * as BbPromise from 'bluebird';

// Model
import { TimeInterval, LeaderboardRecord, ScoreFacet, ScoreFacetData, ScoreFacetTuple, NO_TAG_VALUE } from '../model';
import { ScoreUpdateRecord } from '../repository/write-leaderboard';

// Functions
import { updateScore } from '../repository/write-leaderboard';
import { getDatedScore } from '../util';

export interface InputScoreRecord {
    userId: string
    date: Date
    score: number
    organisationId?: string
    location?: string
    tags?: string[]
}

export const getScoreUpdates = (inputRecords: InputScoreRecord[], intervals: TimeInterval[]) => {
    const explodedScores = _(inputRecords)
        .map(inputRecord => normaliseRecord(inputRecord, intervals))
        .flatMap(explodeScores)
        .value();

    const aggregatedScores = aggregateScores(explodedScores)

    // Promises instantly execute, by wrapping it in an annon
    //  function we can make them lazy
    return aggregatedScores.map(score => () => updateScore(score));
};

interface NormalisedScoreRecord {
    userId: string
    score: number
    date: Date,
    timeIntervals: TimeInterval[]
    scoreFacets: ScoreFacetTuple[]
    tags: string[]
}

const normaliseRecord = (scoreRecord: InputScoreRecord, intervals: TimeInterval[]) => {
    const facets: ScoreFacetTuple[] = [
        [ScoreFacet.ALL, undefined]
    ];

    if (scoreRecord.location) {
        facets.push([ScoreFacet.LOCATION, scoreRecord.location]);
    }

    if (scoreRecord.organisationId) {
        facets.push([ScoreFacet.ORGANISATION, scoreRecord.organisationId])
    }

    const tags = [NO_TAG_VALUE].concat(scoreRecord.tags || []);

    const normalisedScoreRecord: NormalisedScoreRecord = {
        userId: scoreRecord.userId,
        score: scoreRecord.score,
        date: scoreRecord.date,
        timeIntervals: intervals,
        scoreFacets: facets,
        tags: tags,
    };

    return normalisedScoreRecord;
}

const explodeScores = (normalisedRecord: NormalisedScoreRecord) => {
    const { userId, date, timeIntervals, scoreFacets, tags, score } = normalisedRecord;
    
    const scoreUpdateRecords =  _(timeIntervals)
        .map(timeInterval => ({ timeInterval }))
        .flatMap(collection => scoreFacets.map(scoreFacet => Object.assign({}, collection, { scoreFacet })))
        .flatMap(collection => tags.map(tag => Object.assign({}, collection, { tag })))
        .value();

    const userScoreUpdateRecords: ScoreUpdateRecord[]  = _(scoreUpdateRecords)
        .map(collection => Object.assign({}, collection, { userId, score, date }))
        .value();

    return userScoreUpdateRecords;
}

const aggregateScores = (records: ScoreUpdateRecord[]) => {
    const getDatedScoreForUpdateRecord = (scoreUpdateRecord: ScoreUpdateRecord) =>
        getDatedScore(
            scoreUpdateRecord.timeInterval,
            scoreUpdateRecord.date,
            scoreUpdateRecord.scoreFacet[0],
            scoreUpdateRecord.scoreFacet[1],
            scoreUpdateRecord.tag
        )

    const aggregatedScores = _(records)
        .groupBy(scoreUpdateRecord =>
            `${scoreUpdateRecord.userId}-${getDatedScoreForUpdateRecord(scoreUpdateRecord)}`
        )
        .map(similarScoreUpdateRecords =>
            similarScoreUpdateRecords.reduce((acc, { score }) => 
                Object.assign(acc, { score: acc.score + score }))
        )
        .value()

    return aggregatedScores;
}