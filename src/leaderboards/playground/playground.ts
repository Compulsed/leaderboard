
/*
    <timespan>.<time>.<scoreBlock>

    PartitionKey
    <days.10-10-2017.0>, <userId>, <scoreBlock>

    3 Time Intervals
    - Day <day.10-10-2017.0>
    - Month <month.10-2017.0>
    - All Time <allTime.0>

    1 reducer per time interval?

    Query
    --------
    - Give us top 10 users for a given D,M,A
    - Give us a users rep by userId
        - <userId.days.10-10-2017>
    - Give us a users rank by userId --- ?

    Write
    --------
    - Write out rep based on userId into D,M,A

    cd engine/
    yarn install
    sls invoke local --stage mick --region us-east-1 --profile gamification --log --function playground-leaderboard
*/

import 'source-map-support/register'
import { Context, Callback } from 'aws-lambda';

import { TimeInterval, ScoreFacet } from '../model';
import * as leaderboardService from '../services/leaderboard';
import * as leaderboardRepository from '../repository/leaderboard';

const TEST_TYPE = TimeInterval.MONTH;
const TEST_DATE = new Date();
const TEST_TOPN = 170;

const TEST_BOARD = ScoreFacet.ALL 
const TEST_BOARD_DATA = undefined


export const handler = async (event: any, context: Context, cb: Callback) => {
    try {
        const topScores = await leaderboardService.getTop(
            TEST_TYPE,
            TEST_DATE,
            TEST_BOARD,
            TEST_BOARD_DATA,
            TEST_TOPN
        );
    
        console.log(topScores);
        console.log(topScores.length);
        console.log(topScores[topScores.length - 1]);
        cb();
    } catch (err) {
        console.log(err.stack);
    }
};
