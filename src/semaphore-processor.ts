import { adjustSemaphoreCount } from './leaderboards/services/semaphore/semaphore-worker'

export const hander = async (event, context, cb) => {
    try {
        await adjustSemaphoreCount();

        cb(undefined, { message: 'Success!' });
    } catch (err) {
        console.error(err) || cb(err);
    }
}