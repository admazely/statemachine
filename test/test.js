var test = require('tap').test;
var createStatemachine = require('../statemachine');
var redisClient = require('kue').redis.client();

function reset(callback) {
    redisClient.keys('q:*', function(err, keys) {
        if (err) return callback(err);

        var multi = redisClient.multi();
        keys.forEach(function(key) {
            multi.del(key);
        });
        multi.exec(callback);
    });
}

test('a quick job should be finished properly', function(t) {
    t.plan(3);
    reset(function(err) {
        t.equal(err, null, 'reset should not error');
        if (err) return t.end();
        var statemachine = createStatemachine();

        statemachine.process('first', function(data, callback) {
            t.ok(true, 'first job should be processed');
            callback(null);
        });
        statemachine.process('second', function(data, callback) {
            t.ok(true, 'second (last) job should be processed');
            t.end();
        });

        statemachine.createProcedure({}, [{
            name: 'first',
            data: {}
        }, {
            name: 'second',
            data: {}
        }]).execute();
    });
});
