import { Facet } from './facet';

export interface ScoreUpdate {
    userId: string
    score: number
    facets: Facet[]
}