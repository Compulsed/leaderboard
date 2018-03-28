import * as moment from 'moment';
import * as _ from 'lodash';
import * as uuid from 'uuid/v4';
import * as BbPromise from 'bluebird';

// Model
import { LeaderboardRecord, ScoreUpdate } from '../../model';

// Helpers
import { getScoreString, getDatedScoreBlockByScore } from '../../util';

// Functions
import { retryGetUserScore, retryPutUserScore } from '../../repository/write-leaderboard';

// Export
const buildUpdates = (scoreUpdates: ScoreUpdate[]) => {
    return scoreUpdates.map(scoreUpdate => () => updateScore(scoreUpdate));
}

const updateScore = async (scoreUpdateRecord: ScoreUpdate) => {
    const { userId, score, facets } = scoreUpdateRecord;

    const scoreString = getScoreString(facets);

    // Reads the score so the value can be incremented
    const record = await retryGetUserScore(userId, scoreString);

    const currentScore = (record && record.score) || 0;

    const newScore = score + currentScore;

    const newRecord: LeaderboardRecord = {
        userId,
        score: newScore,
        datedScore: scoreString,
        datedScoreBlock: getDatedScoreBlockByScore(facets, newScore),
    };

    await retryPutUserScore(newRecord);
    
    return newRecord;
};

export default buildUpdates;