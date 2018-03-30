export interface WorkerInputScoreUpdate {
    userId: string
    score: number
    facets: { _facetKey: string, _facetValue: string }[]
}