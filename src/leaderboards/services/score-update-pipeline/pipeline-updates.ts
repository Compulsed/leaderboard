import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

import { LeaderboardRecord } from '../../model';

import { PIPELINE_UPDATE_CONCURRENCY } from '../../config';

// export
const pipelineUpdates = (updateTasks: (() => Promise<LeaderboardRecord>)[]) => {              
    return BbPromise.map(
        updateTasks,
        updateTask => updateTask(),
        { concurrency: PIPELINE_UPDATE_CONCURRENCY }
     );
 }
 
export default pipelineUpdates;