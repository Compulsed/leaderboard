import * as _ from 'lodash';

import { Facet, ScoreUpdate, WorkerInputScoreUpdate } from '../../model';

// Functions
import facetFactoryMethod from '../facet-factory-method';

// Export Update Type
const parseFacetEvents = (inputScoreUpdates: WorkerInputScoreUpdate[]) => {
    const scoreUpdates: ScoreUpdate[] = _.map(
        inputScoreUpdates,
        ({ userId, score, facets }) => ({
            userId,
            score,
            facets: _.map(facets, ({ _facetValue, _facetKey }) => facetFactoryMethod(_facetKey, _facetValue))
        })
    );

    return scoreUpdates;
}

export default parseFacetEvents;