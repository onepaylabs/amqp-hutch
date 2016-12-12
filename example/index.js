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

// with this removed the "close" event is not emitted and no retry occurs when rabbit gets bounced
hutch.on('error', function (err) {
  console.log("Error: " + err);
});

function setup(){

  var consumer = function(message, done, fail) {
    console.log("Message Received: " + JSON.parse(message.content));
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
    },
    exclusive: true
  };

  hutch.consume(options, consumer, function(err) {

    if(err){
      console.log(err);
      return;
    }

    console.log('Consumer Setup....');

    hutch.publish(options, "Example Message!", function(err, res){
    });
  });
}


