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

import * as readLeaderRepository from '../repository/read-leaderboard';

export const getTop = async (intervalType: TimeInterval, date: Date, scoreFacet: ScoreFacet, scoreFacetsData: ScoreFacetData, tag: string, topN: number) => {
    const datedScore = getDatedScore(
        intervalType,
        date,
        scoreFacet,
        scoreFacetsData,
        tag
    );

    // TODO: Generate based off of function
    const topScore = 1 * 1000 * 1000;

    const topScoreBlock = getScoreBlockFromScore(topScore);

    let scores:any = [];

    for (var i = topScoreBlock; (i >= 0) && (scores.length < topN); --i)
    {    
        scores = scores.concat(await readLeaderRepository.getScoresInScoreBlock(
            getDatedScoreBlockByBoxIndex(
                intervalType,
                date,
                scoreFacet, 
                scoreFacetsData,
                tag,
                i
            )
        ));
    }

    scores.sort(function sortDescending(a, b){ 
        return b.score - a.score
    });

    return scores.slice(0, topN); 
};
