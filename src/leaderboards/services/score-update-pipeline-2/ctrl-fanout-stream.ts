import { Transform } from 'stream';

import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

import { InputScoreUpdate, ScoreUpdate } from '../../model';
import { TimeFacet, GenericStringFacet } from '../../model';
import { supportedIntervals } from '../../config';

// Functions
import facetFactoryMethod from '../facet-factory-method';

export class TransformStream extends Transform {
    scoreUpdatesBuffer: ScoreUpdate[] = []

    constructor(params) {
        super({ objectMode: true, highWaterMark: 10 });
    }

    exploreScoreUpdate (inputScoreUpdate: InputScoreUpdate) {
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

    async _transform(score: InputScoreUpdate | null, encoding, callback) {
        if (score && this.scoreUpdatesBuffer.length === 0) {
            this.scoreUpdatesBuffer = this.exploreScoreUpdate(score)
        }

        let nextMessage: ScoreUpdate | undefined;

        while (nextMessage = this.scoreUpdatesBuffer.shift()) {
            if (!this.push(nextMessage)) {
                break;
            }
        }
        
        callback();
    }

    async _final(callback) {
        callback();
    }

    async _flush(callback) {
        while (this.scoreUpdatesBuffer.length) {
            await new Promise(resolve => this._transform(null, null, resolve))

            // TODO: Change this logic to make backoffs cleaner
            await BbPromise.delay(1000); 
        }

        callback();
    }

    async _destroy(err, callback) {
        callback();
    }
}
