"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var moment = require("moment");
var model_1 = require("../model");
// returns eg. 1
exports.getScoreBlockFromScore = function (score) {
    return Math.floor(Math.log(score));
};
// returns eg. month_2017/09
exports.getDatedScore = function (intervalType, date, scoreFacet, scoreFacetsData, tag) {
    var momentDate = moment(date);
    var weekDate = Math.ceil(momentDate.date() / 7);
    var getDate = (_a = {},
        _a[model_1.TimeInterval.SECOND] = function () { return momentDate.format('YYYY\/\MM\/DD\/H\/m\/s'); },
        _a[model_1.TimeInterval.MINUTE] = function () { return momentDate.format('YYYY\/\MM\/DD\/H\/m'); },
        _a[model_1.TimeInterval.HOUR] = function () { return momentDate.format('YYYY\/\MM\/DD\/H'); },
        _a[model_1.TimeInterval.DAY] = function () { return momentDate.format('YYYY\/\MM\/DD'); },
        _a[model_1.TimeInterval.WEEK] = function () { return momentDate.format('YYYY\/\MM\/') + weekDate; },
        _a[model_1.TimeInterval.MONTH] = function () { return momentDate.format('YYYY\/\MM'); },
        _a[model_1.TimeInterval.YEAR] = function () { return momentDate.format('YYYY'); },
        _a[model_1.TimeInterval.ALL_TIME] = function () { return 'AT'; },
        _a);
    // TODO: Figure out what to input rather than string type
    var prefix = (_b = {},
        _b[model_1.ScoreFacet.ALL] = function () { return 'all'; },
        _b[model_1.ScoreFacet.LOCATION] = function (location) { return 'loc-' + location; },
        _b[model_1.ScoreFacet.ORGANISATION] = function (organisationId) { return 'org-' + organisationId; },
        _b);
    return tag + "_" + prefix[scoreFacet](scoreFacetsData) + "_" + intervalType + "_" + getDate[intervalType]();
    var _a, _b;
};
// returns eg. month_2017/09_1
exports.getDatedScoreBlockByScore = function (intervalType, date, scoreFacet, scoreFacetsData, tag, score) {
    return exports.getDatedScore(intervalType, date, scoreFacet, scoreFacetsData, tag) + '_' + exports.getScoreBlockFromScore(score);
};
// returns eg. month_2017/09_1
exports.getDatedScoreBlockByBoxIndex = function (intervalType, date, scoreFacet, scoreFacetsData, tag, blockIndex) {
    return exports.getDatedScore(intervalType, date, scoreFacet, scoreFacetsData, tag) + '_' + blockIndex;
};
//# sourceMappingURL=score-helpers.js.map