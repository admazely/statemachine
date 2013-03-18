var kue = require('kue');

// kue/reds open redis clients all over the place - and they do it in module
// scope - so they're not reachable from outside.
// By overriding the createClient function in kue (that is also used in reds) we
// put all clients created in an array so that we can end them later.
var _createClient = kue.redis.createClient.bind(kue.redis);
var redisClients = [];
kue.redis.createClient = function() {
    var client = _createClient();
    redisClients.push(client);
    return client;
}

var test = require('tap').test;
var createStatemachine = require('../statemachine');
var redisClient = kue.redis.client();

var wrapIds = {};
function wrap(str) {
    if (wrapIds[str] === undefined) {
        wrapIds[str] = 0;
    }
    var newStr = str + '-' + wrapIds[str];
    wrapIds[str]++;
    return newStr;
}

function setup(name, _callback) {
    test(name, function(t) {
        var callback = function(err) {
            if (err) {
                t.equal(err, null, 'setup should not error');
                return t.end();
            };

            _callback(t, createStatemachine());
        }
        redisClient.keys('q:*', function(err, keys) {
            if (err) return callback(err);

            var multi = redisClient.multi();
            keys.forEach(function(key) {
                multi.del(key);
            });
            multi.exec(callback);
        });
    });
}

setup('a quick job should finish by using the callback', function(t, statemachine) {
    t.plan(2);
    var first = wrap('first');
    var second = wrap('second');
    statemachine.process(first, function(data, callback) {
        t.ok(true, 'first job should be processed');
        callback(null);
    });
    statemachine.process(second, function(data, callback) {
        t.ok(true, 'second (last) job should be processed');
        t.end();
    });

    statemachine.createProcedure({}, [{
        name: first,
        data: {}
    }, {
        name: second,
        data: {}
    }]).execute();
});

setup('a quick job should finish by calling statemachine.completeJob', function(t, statemachine) {
    t.plan(5);
    var first = wrap('second');
    var second = wrap('second');
    statemachine.process(first, function(job, callback) {
        t.ok(true, 'first job should be processed');
        statemachine.completeJob(job, function(err) {
            t.equal(err, null, 'should not error');
            kue.Job.get(job.id, function(err, job) {
                t.equal(err, null, 'should not error');
                t.equal(job._state, 'complete');
            });
        });
    });
    statemachine.process(second, function(job, callback) {
        t.ok(true, 'second (last) job should be processed');
    });

    statemachine.createProcedure({}, [{
        name: first,
        data: {}
    }, {
        name: second,
        data: {}
    }]).execute();
});

setup('a failed job should be retried when attempts > 0', function(t, statemachine) {
    var i = 0;
    var first = wrap('first');
    statemachine.process(first, function(job, callback) {
        if (i === 0) {
            i++;
            callback(new Error('failure'));
            return;
        }
        t.ok(true, 'should run event more than once');
        t.end();
    });

    statemachine.createProcedure({
        attempts: 2
    }, [{
        name: first,
        data: {}
    }]).execute();
});

setup('resetActiveJobs should fail active jobs#1', function(t, statemachine) {
    t.plan(7);

    var foo = wrap('foo');
    var bar = wrap('bar');
    // start two jobs so that we can be sure that they don't infer with each
    // other
    statemachine.process(foo, function(job1, callback1) {
        statemachine.process(bar, function(job2, callback2) {
            statemachine.resetActiveJobs([foo], function(err) {
                t.equal(err, null, 'should not error');
                if (err) return t.end();

                statemachine.queue.failed(function(err, ids) {
                    t.equal(err, null, 'should not error');
                    if (err) return t.end();

                    t.equal(ids.length, 1, 'should return one failed id');
                    t.equal(job1.id, ids[0], 'failed should have correct id');
                });

                statemachine.queue.active(function(err, ids) {
                    t.equal(err, null, 'should not error');
                    if (err) return t.end();

                    t.equal(ids.length, 1, 'should return one active job');
                    t.equal(job2.id, ids[0], 'active should have correct job');
                });
            });
        });
    });

    // create two separate procedures so that both foo and bar will be
    // processed at the same time above
    statemachine.createProcedure({}, [{
        name: foo,
        data: {}
    }]).execute();
    statemachine.createProcedure({}, [{
        name: bar,
        data: {}
    }]).execute();
});

setup('resetActiveJobs should fail active jobs#2', function(t, statemachine) {
    t.plan(7);
    var foo = wrap('foo');
    var bar = wrap('bar');
    // start two jobs so that we can be sure that they don't infer with each
    // other
    statemachine.process(foo, function(job1, callback1) {
        statemachine.process(bar, function(job2, callback2) {
            statemachine.resetActiveJobs([foo, bar], function(err) {
                t.equal(err, null, 'should not error');
                if (err) return t.end();

                statemachine.queue.failed(function(err, ids) {
                    t.equal(err, null, 'should not error');
                    if (err) return t.end();

                    t.equal(ids.length, 2, 'should return two failed jobs');
                    t.equal(job1.id, ids[0], 'failed should have correct id');
                    t.equal(job2.id, ids[1], 'failed should have correct id');
                });

                statemachine.queue.active(function(err, ids) {
                    t.equal(err, null, 'should not error');
                    if (err) return t.end();

                    t.equal(ids.length, 0, 'should return zero active jobs');
                });
            });
        });
    });

    // create two separate procedures so that both foo and bar will be
    // processed at the same time above
    statemachine.createProcedure({}, [{
        name: foo,
        data: {}
    }]).execute();
    statemachine.createProcedure({}, [{
        name: bar,
        data: {}
    }]).execute();
});

setup('execute() should not delete any attributes', function(t, statemachine) {
    var foo = wrap('foo');
    var procedure = statemachine.createProcedure({}, [{
        name: foo,
        data: {}
    }]);
    t.deepEqual(procedure.steps.length, 1, 'defaults should not be overridden');
    procedure.execute();
    t.deepEqual(procedure.steps.length, 1, 'defaults should not be overridden');
    t.end();
});

test('shutdown', function(t) {
    // end all open redisClients
    redisClients.forEach(function(client) { client.end(); });
    t.end();
});
