var kue = require('kue');

function Procedure(opts) {
    this.queue = opts.queue;
    this.defaults = opts.defaults || {};
    this.steps = opts.steps;
}

Procedure.prototype.execute = function() {
    var self = this;
    // execute the for step in the list
    var currentStep = this.steps.shift();
    var attempts = currentStep.attempts || this.defaults.attempts || 1;

    var job = this.queue.create('statemachine:' + currentStep.name, {
        processData: currentStep.data,
        defaults: self.defaults,
        stepName: currentStep.name,
        // save the rest of the steps, to be executed after this step has finished
        steps: self.steps
    }).attempts(attempts).save();

    job.once('complete', function() {
        // execute the next step
        var procedure = new Procedure({
            queue: self.queue,
            steps: job.data.steps,
            defaults: job.data.defaults
        });
        procedure.execute();
    });

    // this is needed since we have a race condition when the procedure is
    // executed where we sometimes end up subscribing to an event before it
    // has completed
    setTimeout(function() {
        kue.Job.get(job.id, function(err, updatedJob) {
            if (updatedJob._state === 'complete') {
                job.emit('complete');
            }
        });
    }, 100);
}

module.exports = Procedure;