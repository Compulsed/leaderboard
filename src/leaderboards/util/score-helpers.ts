import * as moment from 'moment';
import { TimeInterval, ScoreFacet, ScoreFacetData } from '../model';

// returns eg. 1
export const getScoreBlockFromScore = (score: number) =>
    Math.floor(Math.log(score));

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
export const getDatedScore = (intervalType: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData, tag: string): string => {
    // TODO: Figure out what to input rather than string type
    const prefix: { [scoreFacet: string]: (msc: any) => string } = {
        [ScoreFacet.ALL]: () => 'all',
        [ScoreFacet.LOCATION]: location => 'loc-' + location,
        [ScoreFacet.ORGANISATION]: organisationId => 'org-' + organisationId,        
    };

    return `${tag}_${prefix[scoreFacet](scoreFacetsData)}_${intervalType}_${getDate(intervalType, date)}`;
}

// returns eg. month_2017/09_1
export const getDatedScoreBlockByScore = (intervalType: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData, tag: string, score: number): string => {
    return getDatedScore(intervalType, date, scoreFacet, scoreFacetsData, tag) + '_' + getScoreBlockFromScore(score);
};

// returns eg. month_2017/09_1
export const getDatedScoreBlockByBoxIndex = (intervalType: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData, tag: string, blockIndex: number): string => {
    return getDatedScore(intervalType, date, scoreFacet, scoreFacetsData, tag) + '_' + blockIndex;
};
