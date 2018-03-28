import * as moment from 'moment';
import * as _ from 'lodash';
import * as promiseRetry from 'promise-retry';
import * as AWS from 'aws-sdk';
import * as BbPromise from 'bluebird';

const supportedIntervals = [
    'second',
    'minute',
    'hour',
    'day',
    'week',
    'month',
    'year',
    'allTime',
];

abstract class Facet {
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

class GenericStringFacet extends Facet {
    constructor (facetKey: string, facetValue: string) {
        super(facetKey, facetValue);
    }
}

class TimeFacet extends Facet {
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

const orderFacets = (facets: Facet[]) =>
    _.sortBy(facets, 'facetKey');

const stringifyFacets = (facets: Facet[]) =>
    _(facets)
        .map(facet => facet.makeString())
        .reduce((acc, facetString) => `${acc}${facetString}`, '');

/*
    It is important to order facets so that when they get put into a string, they are all always a consistent order
    Turns facets into their unique string
*/
const getScoreString = (facets: Facet[]): string => {
    const scoreString = _.flow([
        orderFacets,
        stringifyFacets,
    ]);

    return scoreString(facets);
};

// Special mathematical function
const getScoreBlockFromScore = (score: number) =>
    Math.floor(Math.log(score));

// returns eg. <...>_month_2017/09_1 (number is box index literal)
const getGetScoreByBlockIndex = (facets: Facet[], blockIndex: number) =>
    `${getScoreString(facets)}_${blockIndex}`;

// returns eg. <...>_month_2017/09_1 (number is calculated by the blocking function)
const getDatedScoreBlockByScore = (facets: Facet[], score: number) => 
    `${getScoreString(facets)}_${getScoreBlockFromScore(score)}`;

/*
    Normalize Event
*/
type inputFacets = { [facetName: string]: string[] }

interface InputScoreUpdate {
    userId: string
    score: number
    date: string
    inputFacets: inputFacets 
}

/*
    Explode
*/
interface ScoreUpdate {
    userId: string
    score: number
    facets: Facet[]
}

const facetFactoryMethod = (facetKey, facetValue) => {
    const isKeyAnInterval = facetKey =>
        _.includes(supportedIntervals, facetKey)

    if (isKeyAnInterval(facetKey)) {
        return new TimeFacet(facetKey, facetValue);
    }

    return new GenericStringFacet(facetKey, facetValue);
}

const exploreScoreUpdate = (inputScoreUpdate: InputScoreUpdate) => {
    const { date, inputFacets, userId, score } = inputScoreUpdate;

    const inputFacetsWithDates = supportedIntervals
        .map(interval => ({ [interval]: date }));
    
    // Explode all of the scores
    const explodedFacets  = _.reduce(inputFacets, (acc, searchFacetValues, searchFacetKey) =>
        acc.flatMap(searchFacets => [null, ...searchFacetValues].map(
            facetValue => _.assign({}, searchFacets, { [searchFacetKey]: facetValue }))
        ),
        _(inputFacetsWithDates)
    )
    .value();

    // Map over userId and score specific stuff
    const scoreUpdates: ScoreUpdate[] = _.map(
        explodedFacets,
        facetKeyValueArray => ({
            userId,
            score,
            facets: _.map(facetKeyValueArray, (facetValue, facetKey) => facetFactoryMethod(facetKey, facetValue))
        })
    );

    return scoreUpdates;
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
    Repo
*/
interface LeaderboardRecord {
    userId: string
    score: number
    datedScore: string       // TODO: Update to more descriptive name
    datedScoreBlock: string  // TODO: Update to more descriptive name
};


const tableName = process.env.LEADERBOARD_TABLE || 'Unknown';
const indexName = process.env.SCORES_BY_DATED_SCORE_BLOCK_INDEX || 'Unknown';

const docClient = new AWS.DynamoDB.DocumentClient();

const promiseRetryOptions = {
    randomize: true, 
    retries: 10 * 1000, // Should be high enough
    minTimeoutBeforeFirstRetry: 10,
    maxTimeoutBetweenRetries: 1000,
};

const getUserScore = async (userId: string, datedScore: string) => {
    const params = {
        TableName: tableName,
        Key: { userId, datedScore },
        ConsistentRead: true,
    };

    const getResult = await docClient
        .get(params)
        .promise();

    return (getResult.Item || null) as (LeaderboardRecord | null);
}

const putUserScore = (leaderboardRecord: LeaderboardRecord) => {
    const putParams = {
        TableName: tableName,
        Item: leaderboardRecord,
    };

     return docClient
        .put(putParams)
        .promise()
}

const updateScore = async (scoreUpdateRecord: ScoreUpdate) => {
    const { userId, score, facets } = scoreUpdateRecord;

    const scoreString = getScoreString(facets);

    // Reads the score so the value can be incremented
    const record = await promiseRetry(async (retry, number) => {
        return getUserScore(userId, scoreString).catch(err => {
            if (err.code === 'ProvisionedThroughputExceededException') {                
                retry(err);
            }

            throw err;
        });
    }, promiseRetryOptions);

    const currentScore = (record && record.score) || 0;

    const newScore = score + currentScore;

    const newRecord: LeaderboardRecord = {
        userId,
        score: newScore,
        datedScore: scoreString,
        datedScoreBlock: getDatedScoreBlockByScore(facets, newScore),
    };

    await promiseRetry(async (retry, number) => {
        return putUserScore(newRecord).catch(err => {
            if (err.code === 'ProvisionedThroughputExceededException') {                
                retry(err);
            }

            throw err;
        });
    }, promiseRetryOptions);

    return newRecord;
};

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