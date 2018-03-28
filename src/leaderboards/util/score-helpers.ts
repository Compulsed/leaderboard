import { Facet } from '../model';

import * as _ from 'lodash';

const orderFacets = (facets: Facet[]) =>
    _.sortBy(facets, 'facetKey');

const stringifyFacets = (facets: Facet[]) =>
    _(facets)
        .map(facet => facet.makeString())
        .reduce((acc, facetString) => `${acc}${facetString}`, '');

/*
    It is important to order facets so that when they get put into a string, they are all always a consistent order
    Turns facets into their unique string
*/
export const getScoreString = (facets: Facet[]): string => {
    const scoreString = _.flow([
        orderFacets,
        stringifyFacets,
    ]);

    return scoreString(facets);
};

// Special mathematical function
export const getScoreBlockFromScore = (score: number) =>
    Math.floor(Math.log(score));

// returns eg. <...>_month_2017/09_1 (number is box index literal)
export const getGetScoreByBlockIndex = (facets: Facet[], blockIndex: number) =>
    `${getScoreString(facets)}_${blockIndex}`;

// returns eg. <...>_month_2017/09_1 (number is calculated by the blocking function)
export const getDatedScoreBlockByScore = (facets: Facet[], score: number) => 
    `${getScoreString(facets)}_${getScoreBlockFromScore(score)}`;