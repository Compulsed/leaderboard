import 'source-map-support/register'
import { Context, Callback } from 'aws-lambda';
import * as _ from 'lodash';
import * as uuid from 'uuid/v4';
import * as BbPromise from 'bluebird';

import { TimeInterval, ScoreFacet, ScoreFacetData, ScoreFacetTuple } from '../model';
import { getDatedScore } from '../util';

import * as leaderService from '../services/leaderboard';
import * as leaderboardRepository from '../repository/leaderboard';


interface PerformanceEvent {
    userCount?: number
    incrementAmount?: number
    incrementConcurrency?: number
    userConcurrency?: number
}

export const handler = async (event: PerformanceEvent, context: Context, cb: Callback) => {
    const userCount = event.userCount || 5;
    const incrementAmount = event.incrementAmount || 10;
    const incrementConcurrency = event.incrementConcurrency || 3;
    const userConcurrency = event.userConcurrency || 2;

    const timeIntervals = [
        TimeInterval.SECOND,
        TimeInterval.MINUTE,
        TimeInterval.HOUR,
        TimeInterval.DAY,
        TimeInterval.WEEK,
        TimeInterval.MONTH,
        TimeInterval.YEAR,
        TimeInterval.ALL_TIME,
    ];

    const allFacets: ScoreFacetTuple[] = [
        [ScoreFacet.ALL, undefined],
        [ScoreFacet.ORGANISATION, uuid()],
        [ScoreFacet.LOCATION, 'Australia/Melbourne'],
    ];

    const testInfo = [
        'Running performance test',
        ` - User Count: ${userCount}`,
        ` - Increment Amount: ${incrementAmount}`,
        ` - Increment Concurrency: ${incrementConcurrency}`,
        ` - User Concurrency: ${userConcurrency}`,
        ` - Score Facets: ${allFacets.length}`,
        ` - Time Intervals: ${timeIntervals.length}`,
        ``,
        ` Total Scores: ${userCount * allFacets.length * timeIntervals.length}`,        
        ` Total Records Updated: ${userCount * incrementAmount * allFacets.length * timeIntervals.length}`,
    ];

    console.log(testInfo.join('\n'));
    
    try {
        const now = new Date();

        const users = [
            ..._.times(userCount, () => uuid())
        ];

        await BbPromise.map(users, userId =>
            BbPromise.map(
                _.times(incrementAmount, _.identity),
                score => leaderService.updateScore(
                    userId,
                    now,
                    allFacets,
                    ((score || 0) + 1) * 50
                ),
                { concurrency : incrementConcurrency }
            ),
            { concurrency : userConcurrency  }
        );
    
        const scoresWithFacets = allFacets.map(
            ([ scoreFacet, scoreFacetData]) => timeIntervals.map(timeInterval => 
                ({ timeInterval, scoreFacet, scoreFacetData })
            )
        );
        
        const flattenedScoresWithFacets = _.flatten(scoresWithFacets);
    
        const datedScores = flattenedScoresWithFacets
            .map(({ timeInterval, scoreFacet, scoreFacetData }) => 
                getDatedScore(timeInterval, now, scoreFacet, scoreFacetData)
        );

        // <userId, datedScores>
        const usersWithDatedScores = _.mapValues(
            _.keyBy(users),
            userId => leaderboardRepository.getScoresById(userId, _.keyBy(datedScores))
        );

        const results = await BbPromise.props(usersWithDatedScores);

        console.log('RESULTS: ', JSON.stringify(results, null, 2));

        cb(undefined, { message: testInfo });
    } catch(err) {
        console.error(err.stack);
        cb(err);
    }
};
