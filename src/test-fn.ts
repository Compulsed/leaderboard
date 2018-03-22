import 'source-map-support/register';

import * as AWS from 'aws-sdk';
import * as _ from 'lodash';
import * as BbPromise from 'bluebird';
import { groupByMultiple } from './leaderboards/util';

const kinesis = new AWS.Kinesis();

const noRecords = 200
const noRecordSets = 20

const randomNumber = max =>
    Math.floor(Math.random() * max);

const generateRecord = index => {
    const key = String(randomNumber(50));

    const data = {
        userId: key,
        score: 1,
        organisationId: 'test-org',
        location: 'test-loc',
        tags: ['aws', 'ec2']
    };

    const record = {
        PartitionKey: key,
        Data: JSON.stringify({ data }),
    }

    return record;
};

const putRecords = async () => {
    await BbPromise.delay(100);

    const records = _.times(noRecords, generateRecord);

    const putRecords = {
        StreamName: 'kinesis-leaderboard-dev-scoreStream',
        Records: records,
    };

    const kinesisResult = await kinesis
        .putRecords(putRecords)
        .promise();

    console.log(JSON.stringify({ kinesisResult }, null, 2));
};

export const handler = async (event, context, cb) => {
    console.log('In handler!');


    const groupBy = [
        {
            productId: 'associate-bundle',
            courses: ['csa', 'cda', 'csysops']
        },
        {
            productId: 'professional-bundle',
            courses: ['devops-pro', 'pro-csa']
        },
        {
            productId: 'god-mode',
            courses: ['docker', 'csa', 'cda', 'csysops', 'pro-devops', 'pro-csa', ]
        }
    ]

    const multipleGroup = groupByMultiple(groupBy, product => product.courses);

    const multipleGroupWithoutCourses = _(multipleGroup)
        .mapValues(products => products.map(product => _.omit(product, 'courses')))
        .value()

    console.log(JSON.stringify(multipleGroupWithoutCourses, null, 2));

    // await BbPromise.map(
    //     _.times(noRecordSets),
    //     putRecords,
    //     { concurrency: 1 },
    // );

    cb(undefined, { message: 'In Message returned of Lambda'});
};