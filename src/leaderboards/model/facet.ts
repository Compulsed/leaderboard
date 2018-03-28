import * as moment from 'moment';
import * as _ from 'lodash';
import * as AWS from 'aws-sdk';
import * as BbPromise from 'bluebird';

export abstract class Facet {
    protected readonly _facetKey: string
    protected readonly _facetValue: string | null

    constructor (facetKey: string, facetValue: string | null) {
        this._facetKey = facetKey;
        this._facetValue = facetValue;
    }

    makeString(): string {
        return this._facetValue ? `(${this._facetKey}-${this._facetValue})` : '';
    }

    get facetKey(): string { 
        return this._facetKey;
    }

    get facetValue(): string { // ignore 
        return this._facetValue || '';
    }
}

export class GenericStringFacet extends Facet {
    constructor (facetKey: string, facetValue: string) {
        super(facetKey, facetValue);
    }
}

export class TimeFacet extends Facet {
    public static facetKey = 'timeInterval';

    constructor (facetKey: string, facetValue: string) {
        super(facetKey, TimeFacet.facetValueForTimeFacet(facetKey, facetValue));
    }

    private static facetValueForTimeFacet (facetKey: string, facetValue: string) {   
        return TimeFacet.getDate(facetKey, parseInt(facetValue, 10));
    }

    private static getDate(interval: string, timestamp: number) {
        const momentDate = moment(timestamp);
    
        const intervals = {
            second: () => momentDate.format('YYYY\/\MM\/DD\/H\/m\/s'),
            minute: () => momentDate.format('YYYY\/\MM\/DD\/H\/m'),
            hour: () => momentDate.format('YYYY\/\MM\/DD\/H'),
            day: () => momentDate.format('YYYY\/\MM\/DD'),
            week: () => momentDate.format('YYYY\/\MM\/') +  Math.ceil(momentDate.date() / 7),
            month: () => momentDate.format('YYYY\/\MM'),
            year: () => momentDate.format('YYYY'),
            allTime: () => 'AT',
        };

        return intervals[interval]();
    }
}

const explodeScoreUpdates = (inputscoreUpdates: InputScoreUpdate[]) => {
    return inputscoreUpdates.map(exploreScoreUpdate)
}

/*
    Compress
*/
const compressScores = (scoreUpdates: ScoreUpdate[][]) => {
    const flatScoreUpdates = _.flatten(scoreUpdates);

    const groupedScoreUpdates = _.groupBy(
        flatScoreUpdates,
        scoreUpdate => `${scoreUpdate.userId}-${getScoreString(scoreUpdate.facets)}`
    );

    const compressedScoreUpdates = _.map(
        groupedScoreUpdates,
        similarScoreUpdateRecords => similarScoreUpdateRecords.reduce((acc, { score }) => 
            _.assign(acc, { score: acc.score + score })
        )
    );

    return compressedScoreUpdates;
}


/*
    Build Jobs
*/
const buildUpdates = (scoreUpdates: ScoreUpdate[]) => {
    return scoreUpdates.map(scoreUpdate => () => updateScore(scoreUpdate));
}

/*
    Pipeline
*/
const pipelineUpdates = (updateTasks: (() => Promise<LeaderboardRecord>)[]) => {
   return BbPromise.map(
        updateTasks,
        (updateTask: () => Promise<LeaderboardRecord>) => updateTask(),
        { concurrency: 10 }
    );
}

/*
    Runner
*/
const runner = () => {
    const testData: InputScoreUpdate[] = [
        {
            userId: '123',
            score: 10,
            date: Date.now().toString(),
            inputFacets: {
                oraganisationIds: ['333'],
                location: ['location1', 'location2'],
                tags: ['tag1', 'tag2'],
                genders: ['f'],
            },
        },
        {
            userId: '456',
            score: 10,
            date: Date.now().toString(),
            inputFacets: {
                oraganisationIds: ['123'],
                location: ['location1', 'location2'],
                tags: ['tag1', 'tag2'],
            },
        },
        {
            userId: '456',
            score: 5,
            date: Date.now().toString(),
            inputFacets: {
                oraganisationIds: ['123'],
                location: ['location1'],
                tags: ['tag1', 'tag2'],
            },
        },
        {
            userId: '456',
            score: 5,
            date: Date.now().toString(),
            inputFacets: {
                oraganisationIds: ['123'],
                location: ['location1', 'location3'],
                tags: ['tag1'],
                cohorts: ['1', '2', '3'],
                friendsGroups: ['1', '3', '4']
            },
        },        
        {
            userId: '456',
            score: 5,
            date: Date.now().toString(),
            inputFacets: {
                oraganisationIds: ['123'],
                location: ['location1', 'location3'],
                tags: ['tag1'],
                cohorts: ['1', '2', '3'],
                friendsGroups: ['1', '3', '4'],
                genders: ['m']
            },
        }, 
    ];

    const logger = stepName =>
        value => console.log(`Step ${stepName}: `, JSON.stringify(value, null, 2)) || value

    const lengthLogger = stepName =>
        value => console.log(`Step ${stepName}: `, value.length) || value

    const runPipeline = _.flow([
        logger('Input'),
        explodeScoreUpdates,
        lengthLogger('ExplodeScoreUpdates'),
        compressScores,
        lengthLogger('CompressScores'),
        buildUpdates,
        lengthLogger('BuildUpdates'),
        pipelineUpdates,
        lengthLogger('PipelineUpdates'),
    ]);
    
    return runPipeline(testData)
        .then(results => console.log(JSON.stringify({ results }, null, 2)));
}

module.exports.handler = async (event, context, cb) => {
    runner()
        .then(() => cb(undefined))
        .catch(err => cb(err))
}