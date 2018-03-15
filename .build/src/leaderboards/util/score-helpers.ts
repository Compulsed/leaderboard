import * as moment from 'moment';
import { TimeInterval, ScoreFacet, ScoreFacetData } from '../model';

// returns eg. 1
export const getScoreBlockFromScore = (score: number) =>
    Math.floor(Math.log(score));

// returns eg. month_2017/09
export const getDatedScore = (intervalType: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData): string => {
    const momentDate = moment(date);

    const weekDate = Math.ceil(momentDate.date() / 7);

    const getDate = {
        [TimeInterval.SECOND]: () => momentDate.format('YYYY\/\MM\/DD\/H\/m\/s'),
        [TimeInterval.MINUTE]: () => momentDate.format('YYYY\/\MM\/DD\/H\/m'),
        [TimeInterval.HOUR]: () => momentDate.format('YYYY\/\MM\/DD\/H'),
        [TimeInterval.DAY]: () => momentDate.format('YYYY\/\MM\/DD'),
        [TimeInterval.WEEK]: () => momentDate.format('YYYY\/\MM\/') + weekDate,
        [TimeInterval.MONTH]: () => momentDate.format('YYYY\/\MM'),
        [TimeInterval.YEAR]: () => momentDate.format('YYYY'),
        [TimeInterval.ALL_TIME]: () => 'AT',
    };

    // TODO: Figure out what to input rather than string type
    const prefix: { [scoreFacet: string]: (msc: any) => string } = {
        [ScoreFacet.ALL]: () => 'all',
        [ScoreFacet.LOCATION]: location => 'loc-' + location,
        [ScoreFacet.ORGANISATION]: organisationId => 'org-' + organisationId,
    };

    return `${prefix[scoreFacet](scoreFacetsData)}_${intervalType}_${getDate[intervalType]()}`;
}

// returns eg. month_2017/09_1
export const getDatedScoreBlockByScore = (intervalType: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData, score: number): string => {
    return getDatedScore(intervalType, date, scoreFacet, scoreFacetsData) + '_' + getScoreBlockFromScore(score);
};

// returns eg. month_2017/09_1
export const getDatedScoreBlockByBoxIndex = (intervalType: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData, blockIndex: number): string => {
    return getDatedScore(intervalType, date, scoreFacet, scoreFacetsData) + '_' + blockIndex;
};
