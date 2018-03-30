import 'source-map-support/register'
import { Context, Callback } from 'aws-lambda';
import * as _ from 'lodash';

// Model
import { ScoreUpdate, LeaderboardRecord, WorkerInputScoreUpdate } from './leaderboards/model';

// Functions
import { updateScoresFanoutWorker } from './leaderboards/services/score-update-pipeline';

export const handler = async (event: WorkerInputScoreUpdate[], context: Context, cb: Callback) => {
    console.log('Records to progress:', event.length);

    try {
        const results = await updateScoresFanoutWorker(event);

        return cb(undefined, results);
    } catch (err) {
        return console.error(err.message, err.stack) || cb(err);
    }
};
