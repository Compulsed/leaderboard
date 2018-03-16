import * as moment from 'moment';
import * as _ from 'lodash';
import * as uuid from 'uuid/v4';
import * as BbPromise from 'bluebird';

import { 
    TimeInterval,
    LeaderboardRecord,
    ScoreFacet,
    ScoreFacetData,
    ScoreFacetTuple
} from '../model';

import { updateScore } from '../repository/write-leaderboard';

export const getScoreUpdates = (userId: string, date: Date, timeIntervals: TimeInterval[], scoreFacets: ScoreFacetTuple[], tags: string[], amountToUpdate: number) => {
    // TimeIntervals * ScoreFacets
    const scoresWithFacets = scoreFacets.map(
        ([ scoreFacet, scoreFacetData]) =>
            timeIntervals.map(timeInterval =>
                tags.map(tag =>
                    ({ tag, scoreFacet, scoreFacetData, timeInterval })
                )
            )
    );
    
    const flattenedScoresWithFacets = _.flattenDeep(scoresWithFacets);

    // Promises immediate execute, adding an extra annon function allows us to lazily
    //  evaluate. This allows the caller to control how many update tasks are concurrently run
    const scoreUpdates = flattenedScoresWithFacets.map(
        ({ scoreFacet, scoreFacetData, timeInterval, tag }) => () =>
            updateScore(
                userId,
                timeInterval,
                date,
                scoreFacet,
                scoreFacetData,
                tag,
                amountToUpdate
            )
    );

    return scoreUpdates;
};