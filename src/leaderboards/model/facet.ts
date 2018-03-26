
import * as moment from 'moment';
import * as _ from 'lodash';

abstract class Facet {
    protected readonly _facetKey: string
    protected readonly _facetValue: string

    constructor (facetKey: string, facetValue: string) {
        this._facetKey = facetKey;
        this._facetValue = facetValue;
    }

    makeString(): string {
        return `(${this._facetKey}-${this._facetValue})`;
    }

    get facetKey(): string { 
        return this._facetKey;
    }

    get facetValue(): string { // ignore 
        return this._facetValue;
    }
}

class GenericStringFacet extends Facet {
    constructor (facetKey: string, facetValue: string) {
        super(facetKey, facetValue);
    }
}

class TimeFacet extends Facet {
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
        .reduce((acc, facetString) => `${acc}_${facetString}`, '');

const getScoreString = (facets: Facet[]): string => {
    const scoreString = _.flow([
        // It is important to order facets so that when they get put into a string, they are all always a consistent order
        orderFacets,
        // Turns facets into their unique string
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

interface LeaderboardRecord {
    userId: string
    score: number
    datedScore: string
    datedScoreBlock: string
};


/*
    Normalize Event
*/
type inputFacets = { [facetName: string]: string }

interface InputScoreUpdate {
    userId: string
    score: number
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


/*
    Compress
*/

/*
    Build Jobs
*/

/*
    Pipeline
*/