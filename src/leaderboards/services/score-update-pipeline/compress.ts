import * as _ from 'lodash';

import { ScoreUpdate } from '../../model'
import { getScoreString } from '../../util';

// Export
const compressScores = (scoreUpdates: ScoreUpdate[][]) => {
    const flatScoreUpdates = _.flatten(scoreUpdates);

    const groupedScoreUpdates = _.groupBy(
        flatScoreUpdates,
        scoreUpdate => `${scoreUpdate.userId}-${getScoreString(scoreUpdate.facets)}`
    );

    const compressedScoreUpdates = _.map(
        groupedScoreUpdates,
        similarScoreUpdateRecords => similarScoreUpdateRecords.reduce((acc, { score }) => 
            _.assign(acc, { score: acc.score + score })
        )
    );

    return compressedScoreUpdates;
};

export default compressScores;