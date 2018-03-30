import * as _ from 'lodash';

import { InputScoreUpdate, ScoreUpdate } from '../../model';
import { TimeFacet, GenericStringFacet } from '../../model';
import { supportedIntervals } from '../../config';

// Functions
import facetFactoryMethod from '../facet-factory-method';

// Export
const explodeScoreUpdates = (inputscoreUpdates: InputScoreUpdate[]) => {
    return inputscoreUpdates.map(exploreScoreUpdate)
}

const exploreScoreUpdate = (inputScoreUpdate: InputScoreUpdate) => {
    const { date, inputFacets, userId, score } = inputScoreUpdate;

    const inputFacetsWithDates = supportedIntervals
        .map(interval => ({ [interval]: date }));
    
    // Explode all of the scores
    const explodedFacets  = _.reduce(inputFacets, (acc, searchFacetValues, searchFacetKey) =>
        acc.flatMap(searchFacets => [null, ...searchFacetValues].map(
            facetValue => _.assign({}, searchFacets, { [searchFacetKey]: facetValue }))
        ),
        _(inputFacetsWithDates)
    )
    .value();

    // Map over userId and score specific stuff
    const scoreUpdates: ScoreUpdate[] = _.map(
        explodedFacets,
        facetKeyValueArray => ({
            userId,
            score,
            facets: _.map(facetKeyValueArray, (facetValue, facetKey) => facetFactoryMethod(facetKey, facetValue))
        })
    );

    return scoreUpdates;
}

export default explodeScoreUpdates;