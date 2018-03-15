export enum ScoreFacet {
    ALL = 'all',
    ORGANISATION = 'organisation',
    LOCATION = 'location',
};

export type ScoreFacetData = (string | undefined);

export type ScoreFacetTuple = [ScoreFacet, ScoreFacetData];
