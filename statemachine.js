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
        self._formatJobData(job);
        callback(job, function(err) {

            if (err) return done(err);

            self.completeJob(job, done);
        });
    });
}

// TODO: Abstract away this into a lib/job.js-file
StateMachine.prototype._formatJobData = function(job) {
    job.rawData = job.data;
    job.data = helpers.mergeObjs(
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
                                self._formatJobData(job);
                                return job;
                            })
                            .filter(function(job) {
                                return helpers.subset(job.data, queryObj);
                            });

                        callback(null, jobs);
                    });
                }
            );
        });
}

//
// Take all jobs that have state active and set the state to failed instead
// usefull when restarting a process.
// set to failed to that if a job makes a process crash it won't do that forever
//
StateMachine.prototype.failActiveJobs = function(events, callback) {
    if (!Array.isArray(events)) {
        throw new Error('events is not an array');
    }
    var self = this;

    async.forEach(events, function(event, done) {
        var type = 'statemachine:' + event;
        // 0 is first element, -1 is last element
        kue.Job.rangeByType(type, 'active', 0, -1, 'asc', function(err, jobs) {
            if (err) return done(err);

            jobs.forEach(function(job) {
                job.failed();
            });
            done(null);
        });
    }, callback);
}

// TODO: Abstract this to a lib/job.js-file
StateMachine.prototype.completeJob = function(job, callback) {
    callback = callback || function(err) {
        if (err) throw err;
    }

    var data = job.rawData
    if (data.steps.length === 0) return callback(nll);

    // TODO: setting createdNextProcedure as a variable in the job is sort of
    // stupid - but it works and, more importanly, we get a callback we can rely
    // on - that is not the case when you call job.complete.
    if (job.get('createdNextProcedure') === true) return callback(null);

    this.createProcedure(
        data.defaults, data.steps, data.id
    ).execute(function(err, done) {
        if (err) return callback(err);
        job.complete();
        job.set('createdNextProcedure', true);
        job.update(callback);
    });
}

module.exports = StateMachine;
