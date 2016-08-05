var AMQPHutch = require('../');

var hutch = new AMQPHutch();

var options = {
  exchange: {
    name: 'example.exchange.1',
    type: 'topic'
  },
  queue: {
    name: 'example.queue',
    prefetch: 1,
    durable:  true
  },
  publish: {
    persistent: true,
    expiration: 86400000
  }
};

hutch.initialise({
  connectionString: 'amqp://bad',
  retryWait:        100
});

hutch.publish(options, "Example Message!", function(err){
  console.log(err);
});

hutch.on('error', function(err) {
  console.log(err);
});
