var AMQPHutch = require('../');

var hutch = new AMQPHutch();

console.log("Hutch Status:" + hutch.status);

hutch.initialise({
  connectionString: 'amqp://localhost',
  retryWait:        100
});

hutch.on('ready', function() {
  console.log('Established RabbitMQ connection');
  console.log("Hutch Status:" + hutch.status);
  console.log("Hutch Configuration:" + JSON.stringify(hutch.configuration));
  setup();
});

function setup(){

  var consumer = function(message, done, fail) {
    hutch.destroy(options.queue.name, options.exchange.name, function(err){
      console.log("Queue has been destroyed");
      done();
    });
  };

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

  hutch.consume(options, consumer, function(err) {
    console.log('Consumer Setup....');

    hutch.publish(options, "Example Message!", function(err, res){
      console.log("*** Message Sent ***");
    });
  });
}


