const Client = require('node-rest-client').Client;
const moment = require('moment');
const util = require('util');
const Series = require('time-series-data-generator');

const rootUrl = "https://ml.nexosis.com/v1/";


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
    let until = moment.utc().startOf(interval);
    let from = moment.utc().startOf(interval).subtract(100, `${interval}s`);

    var duration = moment.duration(1, `${interval}s`).asSeconds();

    var series = new Series({ from: from.toISOString(), until: until.toISOString(), interval: duration, keyName: "foo"});
    var seriesBar = series.clone({keyName: "bar"});

    var opts = {coefficient: 40, constant: 50,  decimalDigits: 0, period: duration*10};

    var barData = seriesBar.cos(opts)
    data.data = series.sin(opts);

    for(let i = 0; i < data.data.length; i++) {
        data.data[i].bar = barData[i].bar;
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