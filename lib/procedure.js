var kue = require('kue');

var helpers = require('./helpers');

function Procedure(opts) {
    this.queue = opts.queue;
    this.defaults = opts.defaults || {};
    this.steps = opts.steps;
    this.id = opts.id;
}

Procedure.prototype.execute = function(callback) {
    var self = this;
    // execute the for step in the list
    var currentStep = this.steps.shift();
    var attempts = currentStep.attempts || this.defaults.attempts || 1;

    var job = this.queue.create('statemachine:' + currentStep.name, {
        processData: currentStep.data,
        defaults: self.defaults,
        stepName: currentStep.name,
        id: self.id,
        // save the rest of the steps, to be executed after this step has finished
        steps: self.steps
    }).attempts(attempts).save(callback);
}

module.exports = Procedure;