import * as moment from 'moment';
import * as _ from 'lodash';
import { TimeInterval } from '../model';

export type FACET_VALUE = string

export type OPTIONAL_FACET = FACET_VALUE | null;

export interface SearchFacets {
    organisationId?: OPTIONAL_FACET
    location?: OPTIONAL_FACET
    tag?: OPTIONAL_FACET
}

export type PartialSearchFacets = Partial<SearchFacets> 

const facetsToString = (searchFacet: PartialSearchFacets) => _(searchFacet)
    .filter()
    .reduce((acc, facetValue, facetType) => `${acc}${facetType}-${facetValue}_`, '');

// returns eg. 1
export const getScoreBlockFromScore = (score: number) =>
    Math.floor(Math.log(score));

// returns 
export const getDate = (timeInterval, date) => {
    const momentDate = moment(date);

    const getDate = {
        [TimeInterval.SECOND]: () => momentDate.format('YYYY\/\MM\/DD\/H\/m\/s'),
        [TimeInterval.MINUTE]: () => momentDate.format('YYYY\/\MM\/DD\/H\/m'),
        [TimeInterval.HOUR]: () => momentDate.format('YYYY\/\MM\/DD\/H'),
        [TimeInterval.DAY]: () => momentDate.format('YYYY\/\MM\/DD'),
        [TimeInterval.WEEK]: () => momentDate.format('YYYY\/\MM\/') +  Math.ceil(momentDate.date() / 7),
        [TimeInterval.MONTH]: () => momentDate.format('YYYY\/\MM'),
        [TimeInterval.YEAR]: () => momentDate.format('YYYY'),
        [TimeInterval.ALL_TIME]: () => 'AT',
    };

    return getDate[timeInterval];
}

// returns eg. month_2017/09
export const getDatedScore = (intervalType: TimeInterval, date: Date, searchFacet: SearchFacets): string => {
    return `${facetsToString(searchFacet)}_${getDate(intervalType, date)}`;
}

// returns eg. <...>_month_2017/09_1 (number is box index literal)
export const getDatedScoreBlockByBlockIndex = (intervalType: TimeInterval, date: Date, searchFacet: SearchFacets, blockIndex: number): string => {
    return getDatedScore(intervalType, date, searchFacet) + '_' + blockIndex;
};

// returns eg. <...>_month_2017/09_1 (number is calculated by the blocking function)
export const getDatedScoreBlockByScore = (intervalType: TimeInterval, date: Date, searchFacet: SearchFacets, score: number): string => {
    return getDatedScore(intervalType, date, searchFacet) + '_' + getScoreBlockFromScore(score);
};