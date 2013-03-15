
exports.mergeObjs = function() {
    var data = {};
    for (var i = 0; i < arguments.length; ++i) {
        var obj = arguments[i];
        // ignore if it's not an object
        if (!this.isObj(obj)) continue;
        Object.keys(obj).forEach(function(key) {
            data[key] = obj[key];
        });
    }
    return data;
}

exports.copyObj = function(obj) {
    var newObj = {};
    Object.keys(obj).forEach(function(key) {
        newObj[key] = obj[key];
    });
    return newObj;
}

exports.isObj = function(val) {
    return typeof(val) === 'object' && val !== null;
}

exports.flatten = function flatten(obj, values) {
    if (!values) values = [];
    Object.keys(obj).forEach(function(key) {
        var value = obj[key];
        if (!exports.isObj(value)) {
            values.push(value);
            return;
        }
        flatten(value, values);
    });
    return values;
}

exports.subset = function subset(actual, expected) {
    var keys = Object.keys(expected);

    for(var i = 0; i < keys.length; ++i) {
        var key = keys[i];
        if (!exports.isObj(expected[key])) {
            if (expected[key] !== actual[key]) return false;
        } else {
            if (actual === undefined) return false;
            if (!subset(actual[key], expected[key])) return false;
        }
    }
    return true;
}

exports.randomStr = function() {
    return Math.random().toString(8).slice(2);
}
