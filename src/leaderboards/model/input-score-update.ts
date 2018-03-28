type inputFacets = { [facetName: string]: string[] }

export interface InputScoreUpdate {
    userId: string
    score: number
    date: string
    inputFacets: inputFacets 
}