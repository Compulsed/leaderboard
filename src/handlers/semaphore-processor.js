const { adjustSemaphoreCount } = require('../semaphore/semaphore-worker');

const handler = async (event, context, cb) => {
    try {
        await adjustSemaphoreCount();

        cb(undefined, { message: 'Success!' });
    } catch (err) {
        console.error(err) || cb(err);
    }
}

module.exports = { handler }