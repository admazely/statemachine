var async = require('async');

var kue = require('kue');

var reds = require('reds');
// use the same createClient function as kue
reds.createClient = kue.redis.createClient;

var helpers = require('./lib/helpers');
var Procedure = require('./lib/procedure');

function StateMachine(opts) {
    if (!(this instanceof StateMachine)) return new StateMachine(opts);

    //configure which redis connection should be used.
    if (opts.createClient)
        kue.redis.createClient = opts.createClient

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
// reset the active jobs - this should typically be called on initialization.
// The job will first be set to failed, but if there's attempts left then we
// set the state to inactive, so that it can be processed later
//
StateMachine.prototype.resetActiveJobs = function(events, callback) {
    if (!Array.isArray(events)) {
        throw new Error('events is not an array');
    }
    var self = this;

    function resetJob(job, done) {
        job.failed();
        // TODO: Write tests for this part
        job.attempt(function(err, remaining, attempts, max){
            if (err) return done(err);
            if (remaining) {
                job.inactive();
            }
            job.update(done);
        });
    }

    async.forEach(events, function(event, done) {
        var type = 'statemachine:' + event;
        // 0 is first element, -1 is last element
        kue.Job.rangeByType(type, 'active', 0, -1, 'asc', function(err, jobs) {
            if (err) return done(err);

            async.forEach(jobs, resetJob, done);
        });
    }, callback);
}

// TODO: Abstract this to a lib/job.js-file
StateMachine.prototype.completeJob = function(job, callback) {
    callback = callback || function(err) {
        if (err) throw err;
    }

    var data = job.rawData
    job.data = job.rawData; //restore original data object
    if (data.steps.length === 0) return callback(null);

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
