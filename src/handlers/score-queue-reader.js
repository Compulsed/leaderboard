const BbPromise = require('bluebird');
const _ = require('lodash');
const uuidv4 = require('uuid/v4');

const LEADERBOARD_TABLE_NAME = process.env.LEADERBOARD_TABLE || 'Unknown';

const { workerHandler } = require('./score-queue-reader-service/concurrency-helper');
const { docClient } = require('../aws/document-client');

const supportedLeaderboards = [
    'allTime',
    'year',
    'month',
    'week',
    'day',
    'hour',
    'minute'
];

const mapMessages = (sqsMessages) => {
    const mappedMessages = _.map(
        sqsMessages,
        message => JSON.parse(message.Body || '{}')
    );

    console.log(`MappedMessages: ${JSON.stringify(mappedMessages)}`)

    return mappedMessages;
};

const putItem = async (leaderboard, { userId, score /*, date, inputFacets */ }) => {
    const response = await docClient
        .get({
            TableName: LEADERBOARD_TABLE_NAME,
            Key: {
                userId: userId,
                leaderboard: leaderboard,
            },
            ConsistentRead: true,
        })
        .promise();
    
    // If score exists for that leaderboard, increment else initiaise to 0
    const usersCurrentScore = (response && response.Item && response.Item.score) || 0;
    const usersNewScore = usersCurrentScore + score

    await docClient
        .put({
            TableName: LEADERBOARD_TABLE_NAME,
            Item: { 
                userId: userId,                                                                 // PKEY      - Base Table
                leaderboard: leaderboard,                                                      // Sort key  - Base Table
                leaderboardBlock: `${leaderboard}_${Math.floor(Math.log(usersNewScore))}`,     // PKEY      - GSI
                score: usersNewScore,                                                          // Sort Key  - GSI
            }
        })
        .promise();

    return;
};

const putItems = messages => {
    const updates = messages
        .map(message => supportedLeaderboards.map(leaderboard => ({ message, leaderboard }))
    )

    // Multiple 7 * 60 * 10 -> 4200 updates per batch
    const flattenedUpdates = _(updates)
        .flatten()
        .map(update => _.times(10, () => update))
        .flatten()
        .value();

    return _(flattenedUpdates)
        .map(({ message, leaderboard }) => () => putItem(leaderboard, message))
        .shuffle()
        .value();
};

const handler = async (event, context) => {
    return workerHandler(async (messages) => {
        const mappedMessages = mapMessages(messages);

        const updates = putItems(mappedMessages);

        const now = Date.now();

        await BbPromise.map(
            updates,
            update => update(),
            { concurrency: 10 }
        );

        console.log(JSON.stringify({
            totalUpdates: updates.length,
            updatesPerSecond: Math.floor(updates.length / ((Date.now() - now) / 1000)),
        }));
    });
};

module.exports = { handler }