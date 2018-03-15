import 'source-map-support/register';

export const handler = async (event, context, cb) => {
    console.log('Event: ', JSON.stringify({ event, context }, null, 2));

    event.Records.forEach(
        (record, index) => console.log(
            `Record: ${index}, Data: ${Buffer.from(record.kinesis.data, 'base64').toString()}`
        )
    );
    
    cb(undefined, { message: 'In Message returned '});
};