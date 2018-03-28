import * as BbPromise from 'bluebird';

import { LeaderboardRecord } from '../../model';

// export
const pipelineUpdates = (updateTasks: (() => Promise<LeaderboardRecord>)[]) => {
    return BbPromise.map(
         updateTasks,
         (updateTask: () => Promise<LeaderboardRecord>) => updateTask(),
         { concurrency: 10 }
     );
 }
 
export default pipelineUpdates;