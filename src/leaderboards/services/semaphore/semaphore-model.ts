export interface Semaphore {
    semaphore_key: string
    semaphore_sort_key: string
    expires: number | null
}

export const semaphoreTableName = process.env.SEMAPHORE_TABLE || 'Unknown';
export const leaderboardTableName = process.env.LEADERBOARD_TABLE || 'Unknown';

export const semaphoreKey = 'semaphore';