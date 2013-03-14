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

test('shutdown', function(t) {
    // end all open redisClients
    redisClients.forEach(function(client) { client.end(); });
    t.end();
});
