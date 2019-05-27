const BbPromise = require('bluebird');
const _ = require('lodash');
const AWS = require('aws-sdk');
const uuidv4 = require('uuid/v4');

const { obtainSemaphore, countFreeSemaphores } = require('../../semaphore/semaphore');

// Resource Configuration
const QUEUE_URL = process.env.QUEUE_URL;

const invokeNext = () => {
    const lambda = new AWS.Lambda();

    const params = {
        FunctionName: process.env.SELF_FUNCTION || 'Unknown', 
        InvocationType: 'Event', 
    };
    
    return lambda
        .invoke(params)
        .promise();
};

// Invokes more workers based on the count of free semaphores,
//  typically at minium it should invoke at least another to replace itself
const invokeMoreWorkers = async () => {
    const freeSemaphoreCount = await countFreeSemaphores();

    if (freeSemaphoreCount) {
        console.log(`Free semaphore count: ${freeSemaphoreCount}`);

        // Do not invoke more than 5 workers, trade off between slow rampup & invocation spikes
        //  due to read/write concurrency
        var invokeForNWorkers = _.clamp(freeSemaphoreCount, 1, 5);

        console.log(`Invoking ${invokeForNWorkers} more workers`);

        await BbPromise.all(_.times(invokeForNWorkers, invokeNext));

        console.log(`Finished invoking ${invokeForNWorkers} more workers`);
    }

    return;
}

const getMessages = async (size) => {
    const sqs = new AWS.SQS();

    const sqsDequeueParams = {
        QueueUrl: QUEUE_URL,
        MaxNumberOfMessages: size,
    };

    const dequeueResponse = await sqs
        .receiveMessage(sqsDequeueParams)
        .promise();

    console.log(`DequeueResponse: ${JSON.stringify(dequeueResponse)}`)

    return dequeueResponse.Messages || [];
};

const markCompleted = async (sqsMessages) => {
    if (!sqsMessages.length) {
        return;
    }

    const sqs = new AWS.SQS();

    const entries = _.map(
        sqsMessages,
        message => 
            ({
                Id: uuidv4(),
                ReceiptHandle: message.ReceiptHandle,
            })
    );

    const deleteMessage = chunkedEntry => {
        const sqsDeleteParams = {
            QueueUrl: QUEUE_URL,
            Entries: chunkedEntry,
        };
    
        return sqs
            .deleteMessageBatch(sqsDeleteParams)
            .promise();
    }

    const promises = _(entries)
        .chunk(10)
        .map(chunkedEntry => deleteMessage(chunkedEntry))
        .flatten()
        .value();

    const deleteResponses = await Promise.all(promises);

    console.log(JSON.stringify({ deleteResponses }));

    return;
};

const workerHandler = async (workerFunction) => {
    return BbPromise.using(obtainSemaphore(), async () => {
        try {

            const messages = [
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),
                ...(await getMessages(5)),                
            ];

            // There are more messages to process
            if (messages.length > 0) {
                console.log('-- Performing Work');
                await workerFunction(messages);
                console.log('-- Finished Performing Work');

                await markCompleted(messages);

                return true;
            }

            return false;
        } catch (err) {
            console.error(err.message, err.stack);

            return false;
        }
    })
    .then(shouldInvokeMoreWorkers => {
        if (shouldInvokeMoreWorkers) {
            console.log('Likely more messages to process, invoking more workers');
            
            return invokeMoreWorkers();
        }

        console.log('No more messaging to process, going to sleep');
    })
    .catch(err => {
        if (err === 'FAILED_TO_ACQUIRE_SEMAPHORE') {
            console.log('Max attemps reached for Semaphore acquisition');

            return;
        };
    
        console.error('Error in global catch', err);

        return Promise.reject(err);
    })
    .tap(() => console.log('Finished processing'))
};

module.exports = { workerHandler }