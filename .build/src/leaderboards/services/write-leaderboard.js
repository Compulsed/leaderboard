"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var _ = require("lodash");
var write_leaderboard_1 = require("../repository/write-leaderboard");
exports.getScoreUpdates = function (userId, date, timeIntervals, scoreFacets, tags, amountToUpdate) {
    // TimeIntervals * ScoreFacets
    var scoresWithFacets = scoreFacets.map(function (_a) {
        var scoreFacet = _a[0], scoreFacetData = _a[1];
        return timeIntervals.map(function (timeInterval) {
            return tags.map(function (tag) {
                return ({ tag: tag, scoreFacet: scoreFacet, scoreFacetData: scoreFacetData, timeInterval: timeInterval });
            });
        });
    });
    var flattenedScoresWithFacets = _.flattenDeep(scoresWithFacets);
    // Promises immediate execute, adding an extra annon function allows us to lazily
    //  evaluate. This allows the caller to control how many update tasks are concurrently run
    var scoreUpdates = flattenedScoresWithFacets.map(function (_a) {
        var scoreFacet = _a.scoreFacet, scoreFacetData = _a.scoreFacetData, timeInterval = _a.timeInterval, tag = _a.tag;
        return function () {
            return write_leaderboard_1.updateScore(userId, timeInterval, date, scoreFacet, scoreFacetData, tag, amountToUpdate);
        };
    });
    return scoreUpdates;
};
//# sourceMappingURL=write-leaderboard.js.map