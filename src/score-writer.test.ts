import { compressScores } from './score-writer';
import { GenericStringFacet, ScoreUpdate } from './leaderboards/model';

describe('#score-writer', () => { 
    describe('#compress-scores', () => {
        it('compress a group of duplicate scores', () => {
            const userIdOne = '123';
            const userIdTwo = '456';

            const facetsOne: GenericStringFacet[] = [
                new GenericStringFacet('location', 'melbourne'),
                new GenericStringFacet('tag', 'aws'),
                new GenericStringFacet('tag', 'ec2'),
                new GenericStringFacet('tag', 'csa'), // Different
            ];

            const facetsTwo: GenericStringFacet[] = [
                new GenericStringFacet('location', 'melbourne'),
                new GenericStringFacet('tag', 'aws'),
                new GenericStringFacet('tag', 'ec2'),
                new GenericStringFacet('tag', 'cda'), // Different
            ];            

            const scoreUpdates: ScoreUpdate[][] = [
                [
                    { userId: userIdOne, score: 1, facets: facetsOne },
                    { userId: userIdOne, score: 2, facets: facetsTwo },
                    { userId: userIdOne, score: 2, facets: facetsTwo },
                ],
                [
                    { userId: userIdTwo, score: 1, facets: facetsOne },
                    { userId: userIdTwo, score: 2, facets: facetsTwo },
                    { userId: userIdTwo, score: 1, facets: facetsOne },
                ],
            ];

            const compressedUpdates = compressScores(scoreUpdates);

            expect(compressedUpdates.length).toBe(4);
            expect(compressedUpdates[0].score).toBe(1);
            expect(compressedUpdates[1].score).toBe(4); // added 
            expect(compressedUpdates[2].score).toBe(2);
            expect(compressedUpdates[3].score).toBe(2); // added

            console.log(JSON.stringify(compressedUpdates, null, 2));
        })
    });
});