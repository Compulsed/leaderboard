// NOTE: Adding these also requires an update to the TimeFacet formatting
export const supportedIntervals = [
    'second',
    'minute',
    'hour',
    'day',
    'week',
    'month',
    'year',
    'allTime',
];

// 2 -- 60 - 100 w/s
// 4 -- 90 - 100 w/s
export const PIPELINE_UPDATE_CONCURRENCY = 8;

export const LAMBDA_CHUNCK_SIZE = 250; // Should take roughly 1.5 seconds

export const MAX_RECOGNIZABLE_SCORE = 10 * 1000 * 1000;