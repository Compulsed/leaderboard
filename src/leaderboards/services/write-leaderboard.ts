import * as moment from 'moment';
import * as _ from 'lodash';
import * as uuid from 'uuid/v4';
import * as BbPromise from 'bluebird';

// Model
import { TimeInterval, LeaderboardRecord } from '../model';
import { ScoreUpdateRecord } from '../repository/write-leaderboard';
import { SearchFacets, PartialSearchFacets, FACET_VALUE, OPTIONAL_FACET } from '../util';

// Functions
import { updateScore } from '../repository/write-leaderboard';
import { getDatedScore } from '../util';

export interface InputScoreRecord {
    userId: string
    date: Date
    score: number
    organisationId?: string
    locations?: string[]
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
    organisationId: OPTIONAL_FACET,
    locations: FACET_VALUE[],
    tags: FACET_VALUE[],
}

const normaliseRecord = (scoreRecord: InputScoreRecord, intervals: TimeInterval[]) => {
    const normalisedScoreRecord: NormalisedScoreRecord = {
        userId: scoreRecord.userId,
        score: scoreRecord.score,
        date: scoreRecord.date,
        timeIntervals: intervals,
        organisationId: scoreRecord.organisationId || null,
        tags: scoreRecord.tags || [],
        locations: scoreRecord.locations || [],
    };

    return normalisedScoreRecord;
}

const explodeScores = (normalisedRecord: NormalisedScoreRecord) => {
    const { userId, date, timeIntervals, organisationId, locations, tags, score } = normalisedRecord;



    const scoreUpdateRecords =  _(timeIntervals)
        .map(timeInterval => ({ timeInterval }))
        .flatMap(searchFacets => locations.map(location => _.assign({}, searchFacets, { location })))
        .flatMap(searchFacets => tags.map(tag => _.assign({}, searchFacets, { tag })))
        .flatMap(searchFacets => [searchFacets].concat(_.assign({}, searchFacets, { oranisationId: organisationId })))
        .value();

    const userScoreUpdateRecords: ScoreUpdateRecord;

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