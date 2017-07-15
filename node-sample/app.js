const nexosisClient = require('nexosis-api-client').default;
const moment = require('moment');
const util = require('util');
const Series = require('time-series-data-generator');
const csv = require('csv');
const fs = require('fs');

const key = process.env.NEXOSIS_API_KEY;

if(!key) {
    throw new Error("NEXOSIS_API_KEY environment variable must be set");
}

const devNull = err=> {};

const client = new nexosisClient({key: process.env.NEXOSIS_API_KEY});

const action = process.argv[2] || "lastsession";

if(action === "forecast") {
    let interval = 'day';
    let dataSet = 'foo';
    if(process.argv.length >= 4) {
        dataSet = process.argv[3];
    }

    if(process.argv.length >= 5) {
        interval = process.argv[4];
    }

    deleteData(dataSet);
    putData(dataSet, interval)
    postPredictionJob(dataSet, interval, dumpResponse);
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
    let csvPath = undefined;
    
    if(process.argv.length >= 4) {
        csvPath = process.argv[3];
    }

    getSessions(data=> getResultsForLastSession(data, csvPath));
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
    deleteData(name);
    putData(name, 'day', dumpResponse);
}

if(action === "data") {
    if(process.argv.length < 4) {
        listDataSets(dumpResponse);
    }
    else {
        let csvPath = undefined;
        if(process.argv.length >= 5) {
            csvPath = process.argv[4];
        }
        console.log(process.argv, csvPath, process.argv.length);
        getData(process.argv[3], dumpResponse, csvPath);
    }
    
}

function dumpResponse(data) {
    console.log(util.inspect(data, false, null));
}

function deleteData(name) {
    client.DataSets.remove(name);
}

function putData(name, interval, callback) {
    var data = fakeData(interval);

    client.DataSets.create(name, data).then(callback);
}

function getSession(id, callback) {
    client.Sessions.get(id).then(callback);
}

function getLastSession(sessions) {
    const session = sessions.items[sessions.items.length-1];
    getSession(session.sessionId, dumpResponse);
    
}

function getResultsForLastSession(sessions, csvPath) {
    const session = sessions.items[sessions.items.length-1].sessionId;
    getSessionResults(session, dumpResponse, csvPath);
}

function fakeData(interval) {
    let data = {data: []};
    let until = moment.utc().startOf(interval).subtract(1, `${interval}s`);
    let from = moment.utc().startOf(interval).subtract(1, `${interval}s`).subtract(100, `${interval}s`);

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

function postPredictionJob(dataSet, interval, callback) {

    let start = moment.utc().startOf(interval);
    let end = moment.utc().startOf(interval).add(30, `${interval}s`);

    client.Sessions.createForecast(dataSet, start, end, 'foo', interval).then(callback);
}

function getSessions(callback) {
    var args = defaultArgs();

    client.Sessions.list().then(callback);
}



function getSessionResults(id, callback) {
    
    client.Sessions.Get(id).then(callback);

}

function getData(dataSetName, callback) {
    client.DataSets.get(dataSetName).then(callback);
}

function listDataSets(callback) {
    client.DataSets.list().then(callback);
}