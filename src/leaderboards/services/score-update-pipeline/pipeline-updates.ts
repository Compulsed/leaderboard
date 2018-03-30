import * as BbPromise from 'bluebird';
import * as _ from 'lodash';

import { LeaderboardRecord } from '../../model';

import { PIPELINE_UPDATE_CONCURRENCY } from '../../config';

// export
const pipelineUpdates = (updateTasks: (() => Promise<LeaderboardRecord>)[]) => {        
    const randomizedUpdates = _.shuffle(updateTasks);
    
    return BbPromise.map(
        randomizedUpdates,
        (updateTask: () => Promise<LeaderboardRecord>) => updateTask(),
        { concurrency: PIPELINE_UPDATE_CONCURRENCY }
     );
 }
 
export default pipelineUpdates;