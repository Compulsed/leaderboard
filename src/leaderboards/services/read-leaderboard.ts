import * as moment from 'moment';
import * as _ from 'lodash';
import * as uuid from 'uuid/v4';
import * as BbPromise from 'bluebird';

import { Facet, LeaderboardRecord } from '../model';

import { getScoreBlockFromScore, getGetScoreByBlockIndex } from '../util'; 

import * as readLeaderRepository from '../repository/read-leaderboard';
import facetFactoryMethod from './facet-factory-method';

import { MAX_RECOGNIZABLE_SCORE } from '../config';

export const getScores = (timeInterval, date, inputFacets: {}, limit) => {
    const inputFacetsWithTime = _.assign(
        {},
        inputFacets,
        { [timeInterval]: date }
    );
    
    const facets = _.map(inputFacetsWithTime, (facetValue, facetKey) =>
        facetFactoryMethod(facetKey, facetValue))

    return getTopForFacets(facets, limit);
}

export const getTopForFacets = async (facets: Facet[], limit: number) => {
    const topScoreBlock = getScoreBlockFromScore(MAX_RECOGNIZABLE_SCORE);

    let scores: LeaderboardRecord[] = [];

    for (var i = topScoreBlock; (i >= 0) && (scores.length < limit); --i) {    
        scores = scores.concat(await readLeaderRepository.getScoresInScoreBlock(
            getGetScoreByBlockIndex(facets, i)
        ));
    }

    scores.sort((a, b) => b.score - a.score);

    return scores.slice(0, limit); 
};
