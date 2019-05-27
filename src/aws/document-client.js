const AWS = require('aws-sdk');

const https = require('https');

// Configure persistent SSL conection, connection time from 20ms to 5-10ms
const sslAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 50,
  rejectUnauthorized: true,
});

sslAgent.setMaxListeners(0);

AWS.config.update({
  httpOptions: {
    agent: sslAgent,
  },
});

// Configure error timeout interval
const DynamoDBService = new AWS.DynamoDB({ 
    httpOptions: {
        timeout: 1000, // 120000 - default timeout
    },
});


const docClient = new AWS.DynamoDB.DocumentClient({ 
    service: DynamoDBService
});

module.exports = { docClient };