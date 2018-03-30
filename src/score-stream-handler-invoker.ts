import 'source-map-support/register'
import { Context, Callback } from 'aws-lambda';
import * as _ from 'lodash';

// Model
import { InputScoreUpdate } from './leaderboards/model';

// Functions
import { updateScoresFanoutInvoker } from './leaderboards/services/score-update-pipeline';

const formatKinesisRecords = (kinesisRecords) => {
    return kinesisRecords
        .map(record => Buffer.from(record.kinesis.data, 'base64').toString())
        .map(recordJSONString => JSON.parse(recordJSONString))
        .map(_.property('data')) as InputScoreUpdate[]
}

export const handler = async (event: any, context: Context, cb: Callback) => {
    console.log('event', JSON.stringify({ event }, null, 2));

    try {
        const formattedRecords = formatKinesisRecords(event.Records);

        await updateScoresFanoutInvoker(formattedRecords);

        return cb(undefined, { message: 'Success' });
    } catch (err) {
        return console.error(err.message, err.stack) || cb(err);
    }
};
