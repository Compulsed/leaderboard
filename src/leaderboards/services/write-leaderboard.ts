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

const scoreWriteConcurrency = 8;

export const updateScore = async (userId: string, date: Date, timeIntervals: TimeInterval[], scoreFacets: ScoreFacetTuple[], amountToUpdate: number) => {
    // TimeIntervals * ScoreFacets
    const scoresWithFacets = scoreFacets.map(
        ([ scoreFacet, scoreFacetData]) => timeIntervals.map(timeInterval => 
            ({ scoreFacet, scoreFacetData, timeInterval })
        )
    );
    
    const flattenedScoresWithFacets = _.flatten(scoresWithFacets);

    const updatedScores = await BbPromise.map(
        flattenedScoresWithFacets,
        ({ scoreFacet, scoreFacetData, timeInterval }) => writeLeaderRepository.updateScore(
            userId,
            timeInterval,
            date,
            scoreFacet,
            scoreFacetData,
            amountToUpdate
        ),
        { concurrency: scoreWriteConcurrency } 
    );

    return updatedScores;
}