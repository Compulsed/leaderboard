import 'source-map-support/register'

import * as BbPromise from 'bluebird';
import * as _ from 'lodash';
import * as fs from 'fs';

import { Readable } from 'stream';
import { InputScoreUpdate } from '../../model';

import * as AWS from 'aws-sdk';

const sqs = new AWS.SQS();

// TODO:
const queueUrl = 'https://sqs.us-east-1.amazonaws.com/145722906259/scoreQueue.fifo';

const timeoutPeriodInMilliseconds = 3 * 60 * 1000

export class ReadStream extends Readable {
    messageBuffer: InputScoreUpdate[] = []
    timeLeftFunction: () => Number // TODO: Consider making a ping stream

    constructor(params) {
        super({ objectMode: true, highWaterMark: 10 });
        this.timeLeftFunction = params.timeLeftFunction;
    }

    isTimeLeft () {
        return timeoutPeriodInMilliseconds < this.timeLeftFunction()
    }

    async getMessages(size) {
        const sqsDequeueParams = {
            QueueUrl: queueUrl,
            MaxNumberOfMessages: size,
        };

        const dequeueResponse = await sqs
            .receiveMessage(sqsDequeueParams)
            .promise();

        const mappedMessages: InputScoreUpdate[] = _.map(
            dequeueResponse.Messages || [],
            message => JSON.parse(message.Body || '{}') // TODO: handle missing body
        );

        return mappedMessages;
    }

    // Size is how many values
    async _read(size) {
        console.log(`Read: ${size}`);

        while (this.messageBuffer.length === 0 && this.isTimeLeft()) {
            // TODO: Change this logic to make backoffs cleaner
            await BbPromise.delay(1000); 

            if (this.isTimeLeft()) {
                this.messageBuffer = await this.getMessages(size);
            }
        }

        let nextMessage: InputScoreUpdate | undefined;

        while (nextMessage = this.messageBuffer.shift()) {
            if (!this.push(nextMessage)) {
                break;
            }
        }

        // We are running out of time, let the stream finish
        if (!this.isTimeLeft()) {
            this.push(null);
        }

        return;
    }
}
