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

import { 
    getDatedScore,
    getScoreBlockFromScore,
    getDatedScoreBlockByBoxIndex,
    getDatedScoreBlockByScore
} from '../util';

import * as leaderRepository from '../repository/leaderboard';

export const updateScore = async (userId: string, date: Date, scoreFacets: ScoreFacetTuple[], amountToUpdate: number) => {
    const timeIntervals = [
        // TimeInterval.SECOND,
        // TimeInterval.MINUTE,
        TimeInterval.HOUR,
        TimeInterval.DAY,
        TimeInterval.WEEK,
        TimeInterval.MONTH,
        TimeInterval.YEAR,
        TimeInterval.ALL_TIME,
    ];

    const scoresWithFacets = scoreFacets.map(
        ([ scoreFacet, scoreFacetData]) => timeIntervals.map(timeInterval => 
            ({ scoreFacet, scoreFacetData, timeInterval })
        )
    );
    
    const flattenedScoresWithFacets = _.flatten(scoresWithFacets);

    const scoreIncrementPromises = _.map(
        flattenedScoresWithFacets,
        ({ scoreFacet, scoreFacetData, timeInterval }) => leaderRepository.updateScore(
            userId,
            timeInterval,
            date,
            scoreFacet,
            scoreFacetData,
            amountToUpdate
        )
    );

    return BbPromise.all(scoreIncrementPromises);
}

export const getTop = async (intervalType: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData, topN: number) => {
    const datedScore = getDatedScore(
        intervalType,
        date,
        scoreFacet,
        scoreFacetsData
    );

    // TODO: Generate based off of function
    const topScore = 1000;

    const topScoreBlock = getScoreBlockFromScore(topScore);

    let scores:any = [];

    for (var i = topScoreBlock; (i >= 0) && (scores.length < topN); --i)
    {    
        scores = scores.concat(await leaderRepository.getScoresInScoreBlock(
            getDatedScoreBlockByBoxIndex(
                intervalType,
                date,
                scoreFacet, 
                scoreFacetsData,
                i
            )
        ));
    }

    scores.sort(function sortDescending(a, b){ 
        return b.score - a.score
    });

    return scores.slice(0, topN); 
};
