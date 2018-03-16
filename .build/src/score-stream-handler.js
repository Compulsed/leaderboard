"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = y[op[0] & 2 ? "return" : op[0] ? "throw" : "next"]) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [0, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var _this = this;
Object.defineProperty(exports, "__esModule", { value: true });
require("source-map-support/register");
var BbPromise = require("bluebird");
var _ = require("lodash");
var model_1 = require("./leaderboards/model");
var write_leaderboard_1 = require("./leaderboards/services/write-leaderboard");
// Defines the amount of update tasks that can be running at one time,
//  an update task involves (1 Read & 1 Write)
var recordConcurrencyLevel = 10;
var timeIntervals = [
    model_1.TimeInterval.HOUR,
    model_1.TimeInterval.DAY,
    model_1.TimeInterval.WEEK,
    model_1.TimeInterval.MONTH,
    model_1.TimeInterval.YEAR,
    model_1.TimeInterval.ALL_TIME,
];
var tags = [
    'NONE',
    'csa',
    'ec2',
];
var processRecord = function (scoreRecord) {
    var facets = [
        [model_1.ScoreFacet.ALL, undefined]
    ];
    if (scoreRecord.location) {
        facets.push([model_1.ScoreFacet.LOCATION, scoreRecord.location]);
    }
    if (scoreRecord.organisationId) {
        facets.push([model_1.ScoreFacet.ORGANISATION, scoreRecord.organisationId]);
    }
    var scoreUpdates = write_leaderboard_1.getScoreUpdates(scoreRecord.userId, new Date(), timeIntervals, facets, tags, scoreRecord.score);
    return scoreUpdates;
};
var aggregateScores = function (records) {
    return _(records)
        .groupBy('userId')
        .map(function (scores) { return scores.reduce(function (acc, _a) {
        var score = _a.score;
        return Object.assign(acc, { score: acc.score + score });
    }); })
        .value();
};
var formatKinesisRecords = function (kinesisRecords) {
    return kinesisRecords
        .map(function (record) { return Buffer.from(record.kinesis.data, 'base64').toString(); })
        .map(function (recordJSONString) { return JSON.parse(recordJSONString); })
        .map(_.property('data'));
};
// Total Writes + Reads
//  (Tags) * (Facets) * (TimeIntervals) * (Number of Records)
//  3 * 2 * 6 * 10
exports.handler = function (event, context, cb) { return __awaiter(_this, void 0, void 0, function () {
    var formattedRecords, aggregatedScores, scoreUpates, processedRecords, err_1;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                console.log('event', JSON.stringify({ event: event }, null, 2));
                _a.label = 1;
            case 1:
                _a.trys.push([1, 3, , 4]);
                formattedRecords = formatKinesisRecords(event.Records);
                console.log(JSON.stringify({ formattedRecords: formattedRecords }, null, 2));
                aggregatedScores = aggregateScores(formattedRecords);
                console.log(JSON.stringify({ aggregatedScores: aggregatedScores }, null, 2));
                scoreUpates = _(aggregatedScores)
                    .map(processRecord)
                    .flatten()
                    .value();
                return [4 /*yield*/, BbPromise.map(scoreUpates, function (scoreUpdate) { return scoreUpdate(); }, { concurrency: recordConcurrencyLevel })];
            case 2:
                processedRecords = _a.sent();
                console.log(JSON.stringify({ processedRecords: processedRecords }, null, 2));
                return [2 /*return*/, cb(undefined, { message: 'Success' })];
            case 3:
                err_1 = _a.sent();
                return [2 /*return*/, console.error(err_1.message, err_1.stack) || cb(err_1)];
            case 4: return [2 /*return*/];
        }
    });
}); };
//# sourceMappingURL=score-stream-handler.js.map