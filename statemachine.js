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

StateMachine.prototype.createProcedure = function(defaults, steps, id) {
    return new Procedure({
        defaults: defaults,
        queue: this.queue,
        steps: steps,
        // random and unique id for this procedure - used to resume already
        // started jobs, among other things
        id: id || helpers.randomStr()
    });
}

StateMachine.prototype.process = function(event, concurrency, callback) {
    var self = this;

    if (!callback) {
        callback = concurrency;
        concurrency = 1;
    }

    this.queue.process('statemachine:' + event, concurrency, function(job, done) {
        var data = job.data;
        job.data = self._formatJobData(job);
        callback(job, function(err) {

            if (err) return done(err);

            if (data.steps.length === 0) return done(null);

            self.createProcedure(
                data.defaults, data.steps, data.id
            ).execute(done);
        });
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
                                job.rawData = job.data;
                                job.data = self._formatJobData(job);
                                return job;
                            })
                            .filter(function(job) {
                                // note: It would be nice if kue had a way to get this
                                // info without looking at an internal variable 

                                return helpers.subset(job.data, queryObj);
                            });

                        callback(null, jobs);
                    });
                }
            );
        });
}

// Probably don't include this
StateMachine.prototype.resumeProcedures = function(queryObj, callback) {
    this.query(queryObj, function(err, jobs) {
        var procedures = {};
        jobs.forEach(function(job) {
            var id = job.rawData.id;
            procedures[id] = procedures[id] || [];
            procedures.push(job);
        });
    });
}

module.exports = StateMachine;
