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

import * as writeLeaderRepository from '../repository/write-leaderboard';

export const getScoreUpdates = (userId: string, date: Date, timeIntervals: TimeInterval[], scoreFacets: ScoreFacetTuple[], amountToUpdate: number) => {
    // TimeIntervals * ScoreFacets
    const scoresWithFacets = scoreFacets.map(
        ([ scoreFacet, scoreFacetData]) => timeIntervals.map(timeInterval => 
            ({ scoreFacet, scoreFacetData, timeInterval })
        )
    );
    
    const flattenedScoresWithFacets = _.flatten(scoresWithFacets);

    // Promises immediate execute, adding an extra annon function allows us to lazily
    //  evaluate. This allows the caller to control how many update tasks are concurrently run
    const scoreUpdates = flattenedScoresWithFacets.map(
        ({ scoreFacet, scoreFacetData, timeInterval }) => () => 
        writeLeaderRepository.updateScore(
            userId,
            timeInterval,
            date,
            scoreFacet,
            scoreFacetData,
            amountToUpdate
        )
    );

    return scoreUpdates;
};