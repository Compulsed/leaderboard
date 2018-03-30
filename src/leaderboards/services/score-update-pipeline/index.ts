import * as _ from 'lodash';

import { InputScoreUpdate, ScoreUpdate, LeaderboardRecord, WorkerInputScoreUpdate } from '../../model';

import explodeScoreUpdates from './explode';
import compressScores from './compress'
import buildUpdates from './build-updates';
import pipelineUpdates from './pipeline-updates'
import fanoutUpdates from './fanout-updates';
import parseFacetEvents from './parse-facet-events';

// -- Single, worker + invoker
const updateScores = (inputScores: InputScoreUpdate[]): Promise<LeaderboardRecord[]> => {
    const runPipeline = _.flow([
        logger('Input'),
        explodeScoreUpdates,
        lengthLogger('ExplodeScoreUpdates'),
        compressScores,
        lengthLogger('CompressScores'),
        buildUpdates,
        lengthLogger('BuildUpdates'),
        pipelineUpdates,
        lengthLogger('PipelineUpdates'),
    ]);

    return runPipeline(inputScores);
};

// -- Pair, invoker
export const updateScoresFanoutInvoker = (inputScores: InputScoreUpdate[]): Promise<LeaderboardRecord[]> => {
    const runPipeline = _.flow([
        logger('Input'),
        explodeScoreUpdates,
        lengthLogger('ExplodeScoreUpdates'),
        compressScores,
        lengthLogger('CompressScores'),
        fanoutUpdates, // Calc into a lambda which calls `updateScoresFanoutWorker` function
        lengthLogger('FanoutUpdates'),
    ]);

    return runPipeline(inputScores);
};

// -- Pair, worker
export const updateScoresFanoutWorker = (scoreUpdates: WorkerInputScoreUpdate[]): Promise<LeaderboardRecord[]> => {
    const runPipeline = _.flow([
        logger('Input'),
        parseFacetEvents, // Required because we need to reconstruct facet classes from DTO
        lengthLogger('ParseFacetEvents'),
        buildUpdates,
        lengthLogger('BuildUpdates'),
        pipelineUpdates,
        lengthLogger('PipelineUpdates'),
    ]);

    return runPipeline(scoreUpdates);
};

const logger = stepName =>
    value => Promise.resolve(value)
        .then(value => console.log(`Step ${stepName}: `, JSON.stringify(value, null, 2))) && value

const lengthLogger = stepName =>
    value => Promise.resolve(value)
        .then(value => console.log(`Step ${stepName}: `, value.length)) && value

export default updateScores;