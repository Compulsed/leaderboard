import * as _ from 'lodash';

import { InputScoreUpdate } from '../../model';

import explodeScoreUpdates from './explode';
import compressScores from './compress'
import buildUpdates from './build-updates';
import pipelineUpdates from './pipeline-updates'

// export
const updateScores = (inputScores: InputScoreUpdate[]) => {
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
}

const logger = stepName =>
    value => console.log(`Step ${stepName}: `, JSON.stringify(value, null, 2)) || value

const lengthLogger = stepName =>
    value => console.log(`Step ${stepName}: `, value.length) || value

export default updateScores;