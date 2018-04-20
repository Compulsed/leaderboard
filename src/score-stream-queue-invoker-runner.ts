import * as BbPromise from 'bluebird';

import { handler } from './score-stream-queue-invoker';

handler(
    {},
    { getRemainingTimeInMillis: () => 1000 * 60 * 10 },
    () => console.log('DONE!')
);

(async () => {
    BbPromise.delay(30 * 1000)
})()