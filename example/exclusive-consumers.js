var AMQPHutch = require('../');

var hutch = new AMQPHutch();
var hutch2 = new AMQPHutch();

hutch.initialise({
  connectionString: 'amqp://localhost',
  retryWait:        2000
});

hutch2.initialise({
  connectionString: 'amqp://localhost',
  retryWait:        2000
});

hutch.on('ready', function() {
  console.log('Established RabbitMQ connection');
  setup();
});

hutch2.on('ready', function() {
  console.log('Established RabbitMQ connection 2');
  setup2();
});

hutch.on('error', function (err) {
  console.log("Error: " + err);
});

hutch2.on('error', function (err) {
  console.log("Error: " + err);
});

function setup(){

  var consumer = function(message, done, fail) {
    console.log(JSON.parse(message.content));
    done();
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
    if(!err) console.log('Consumer 1 Setup....');

    hutch.publish(options, "Example Message!", function(err, res){
      console.log("*** Message Sent ***");
    });
  });
}

function setup2(){

  var consumer = function(message, done, fail) {
    console.log(JSON.parse(message.content));
    done();
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

  hutch2.consume(options, consumer, function(err) {
    if(!err) console.log('Consumer 1 Setup....');

    hutch2.publish(options, "Example Message!", function(err, res){
      console.log("*** Message Sent ***");
    });
  });
}



