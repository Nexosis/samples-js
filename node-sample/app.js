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

    client.DataSets.remove(dataSet)
        .then(()=> {return putData(dataSet, interval)})
        .then(() => {return postPredictionJob(dataSet, interval)})
        .then(data=>dumpResponse(data))
        .catch(writeError);
}

if(action === "sessions") {
    client.Sessions.list().then(dumpResponse).catch(writeError);
}

if(action === "getsession") {
    client.Sessions.get(process.argv[3]).then(dumpResponse).catch(writeError);
}

if(action === "lastsession") {
    client.Sessions.list()
        .then(sessions=>{return sessions.items[sessions.items.length-1];})
        .then(session=> {return client.Sessions.get(session.sessionId);})
        .then(dumpResponse)
        .catch(writeError);
}

if(action === "results") {
    client.Sessions.list()
        .then(sessions=>{return sessions.items[sessions.items.length-1];})
        .then(session=> {return client.Sessions.results(session.sessionId);})
        .then(dumpResponse)
        .catch(writeError);
}

if(action == "put") {
    let name = "foo";
    if(process.argv.length >= 4) {
        name = process.argv[3];
    }
    client.DataSets.remove(name);
    putData(name, 'day')
        .then(dumpResponse)
        .catch(writeError);
}

if(action === "data") {

    if(process.argv.length < 4) {
        client.DataSets.list()
            .then(dumpResponse)
            .catch(writeError);
    }
    else {
        client.DataSets.get(process.argv[3])
            .then(dumpResponse)
            .catch(writeError);
    }
    
}

function dumpResponse(data) {
    console.log(util.inspect(data, false, null));
}

function writeError(err) {
    console.log(err);
}

function putData(name, interval) {
    console.log('putting');
    var data = fakeData(interval);

    return client.DataSets.create(name, data);
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

function postPredictionJob(dataSet, interval) {
    console.log('predicting');
    let start = moment.utc().startOf(interval);
    let end = moment.utc().startOf(interval).add(30, `${interval}s`);

    return client.Sessions.createForecast(dataSet, start.format(), end.format(), 'foo', interval);
}

function listDataSets(callback) {
    client.DataSets.list().then(callback);
}