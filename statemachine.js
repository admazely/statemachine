var async = require('async');

var kue = require('kue');

var reds = require('reds');
// use the same createClient function as kue
reds.createClient = kue.redis.createClient;

var helpers = require('./lib/helpers');
var Procedure = require('./lib/procedure');

function StateMachine() {
    if (!(this instanceof StateMachine)) return new StateMachine();
    this.queue = kue.createQueue();

    // q:search is the reds-namespace created by kue, so the queue are
    // aumatically indexed
    this.search = reds.createSearch('q:search');
}

StateMachine.prototype.createProcedure = function(defaults, steps) {
    return new Procedure({
        defaults: defaults,
        queue: this.queue,
        steps: steps
    });
}

StateMachine.prototype.process = function(event, concurrency, callback) {
    var self = this;

    if (!callback) {
        callback = concurrency;
        concurrency = 1;
    }

    console.log('event', event);
    this.jobs.process('statemachine:' + event, concurrency, function(job, done) {
        job.data = self._formatJobData(job);
        callback(job, done);
    });
}

StateMachine.prototype._formatJobData = function(job) {
    return helpers.mergeObjs(
        {
            stepName: job.data.stepName
        },
        job.data.defaults,
        job.data.processData
    );
}

//
// Query the none-completed jobs with a json-object, such that the specified
// json object is subset of the found job-data
//
StateMachine.prototype.query = function(queryObj, callback) {
    var self = this;

    // flatten the queryObj and use that as the search string
    // reds does a AND search as standard between the different values
    var searchString = helpers.flatten(queryObj).join(' ');

    this.search
        .query(searchString)
        .end(function(err, ids) {
            async.map(
                ids,
                function(id, done) {
                    kue.Job.get(id, done);
                },
                function(err, jobs) {
                    process.nextTick(function() {
                        if (err) return callback(err);

                        jobs = jobs
                            .map(function(job) {
                                job.data = self._formatJobData(job);
                                return job;
                            })
                            .filter(function(job) {
                                // note: It would be nice if kue had a way to get this
                                // info without looking at an internal variable 
                                if (job._state === 'complete') return false;

                                return helpers.subset(job.data, queryObj);
                            });

                        callback(null, jobs);
                    });
                }
            );
        });
}

module.exports = StateMachine;