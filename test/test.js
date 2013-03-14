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

setup('a quick job should be finished properly', function(t, statemachine) {
    t.plan(2);
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

test('shutdown', function(t) {
    // end all open redisClients
    redisClients.forEach(function(client) { client.end(); });
    t.end();
});
