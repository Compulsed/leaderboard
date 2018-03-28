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