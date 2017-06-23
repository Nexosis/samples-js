const Client = require('node-rest-client').Client;
const moment = require('moment');
const util = require('util');

const rootUrl = "https://ml.nexosis.com/api/";



const key = process.env.NEXOSIS_API_KEY;

if(!key) {
    throw new Error("NEXOSIS_API_KEY environment variable must be set");
}

const client = new Client();

const action = process.argv[2] || "lastsession";

if(action === "forecast") {
    let interval = 'day';
    if(process.argv.length >= 4) {
        interval = process.argv[3];
    }
    postPredictionJob(interval, dumpResponse);
}

if(action === "sessions") {
    getSessions(dumpResponse);
}

if(action === "getsession") {
    getSession(process.argv[3], dumpResponse);
}

if(action === "lastsession") {
    getSessions(data=> getLastSession(data));
}

if(action === "results") {
    getSessions(data=> getResultsForLastSession(data));
}

if(action === "getforecast") {
    getForecast(process.argv[3], dumpResponse);
}

if(action === "model") {
    getModel(process.argv[3], dumpResponse);
}

if(action == "put") {
    let name = "foo";
    if(process.argv.length >= 4) {
        name = process.argv[3];
    }

    putData(name, dumpResponse);
}

if(action === "data") {
    if(process.argv.length < 4) {
        listDataSets(dumpResponse);
    }
    else {
        getData(process.argv[3], process.argv[4], dumpResponse);
    }
    
}

function dumpResponse(data, response) {
    console.log(response.headers);
    if(response.statusCode !== 200) {
        console.log(response);
    }
    console.log(util.inspect(data, false, null));
}

function putData(name, callback) {
    let args = defaultArgs();
    args.data = fakeData();

    client.put(`${rootUrl}data/${name}`, args, function(data, response) {
        callback(data, response);
    });

}

function getSession(id, callback) {
    let args = defaultArgs();
    client.get(`${rootUrl}sessions/${id}`, args, function(data, response) {
        callback(data, response);
    });
}

function getLastSession(sessions) {
    const session = sessions.items[sessions.items.length-1];
    getSession(session.sessionId, dumpResponse);
    
}

function getResultsForLastSession(sessions) {
    const session = sessions.items[sessions.items.length-1].sessionId;
    getSessionResults(session, dumpResponse);
}

function defaultArgs() {
    var args = {
        headers: { "api-key": key, "Content-Type": "application/json" },
    };
    return args;
}


function fakeData(interval) {
    let data = {data: []};
    let start = moment.utc().startOf(interval);

    let mult = 1;
    const WAVE_MAGNITUDE = 20;
    for(var i = 0; i < 100; i++) {
        if((i % WAVE_MAGNITUDE) === 0) {
            mult *= -1;
        }
        var point = (i % WAVE_MAGNITUDE) * mult;
        data.data.push({timestamp: start.subtract(1, `${interval}s`).format(), values: {foo : 100 + point, bar: 200 + point }});
    }
    return data;
}

function postPredictionJob(interval, callback) {
    var args = defaultArgs();
    args.data = fakeData(interval);

    let start = moment.utc().startOf(interval);
    let end = moment.utc().startOf(interval).add(30, `${interval}s`);


    client.post(`${rootUrl}sessions/forecast?startDate=${start.toISOString()}&endDate=${end.toISOString()}&targetColumn=foo&resultInterval=${interval}`, args, function (data, response) {
    	callback(data, response);
    });
}

function getSessions(callback) {
    var args = defaultArgs();
    client.get(`${rootUrl}sessions`, args, function(data, response) {
        callback(data, response);
    });
}

function getSessionResults(id, callback) {
    var args = defaultArgs();
    client.get(`${rootUrl}sessions/${id}/results`, args, function(data, response) {
        callback(data, response);
    });
}

function getModel(dataSetName, callback) {
    var args = defaultArgs();
    let url = `${rootUrl}data/${dataSetName}/forecast/model`;
    client.get(url, args, function(data, response) {
        callback(data, response);
    });
}

function getForecast(dataSetName, callback) {
    var args = defaultArgs();
    let url = `${rootUrl}data/${dataSetName}/forecast`;
    client.get(url, args, function(data, response) {
        callback(data, response);
    });
}

function getData(dataSetName, queryString, callback) {
    var args = defaultArgs();
    let url = `${rootUrl}data/${dataSetName}`;
    if(args) {
        url = `${url}?${queryString}`;
    }
    client.get(url, args, function(data, response) {
        callback(data, response);
    });
}

function listDataSets(callback) {
    var args = defaultArgs();
    client.get(`${rootUrl}data`, args, function(data, response) {
        callback(data, response);
    });
}