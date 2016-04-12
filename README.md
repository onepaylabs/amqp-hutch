# AQMP:Hutch
[amqplib](https://www.npmjs.com/package/amqplib) wrapper for easy setup and initialization.

## Setup
Configuration and Events for AMQP Hutch.
```javascript
var hutch     = require('amqp-hutch');

hutch.initialise({
  connectionString: 'amqps://user:password@host:port/uri?heartbeat=3',
  retryWait:        1000
});

hutch.on('ready', function(){
  console.log('Established RabbitMQ connection');
});

hutch.on('close', function(err){
  console.log(err.message + 'RabbitMQ closing connection');
});

hutch.on('error', function(err){
  console.log(err.message + 'RabbitMQ connection error');
});
```

## Consume
Consume creates a queue bound to a new channel.
```javascript
  var options = {
    queue: 'queue.name',
    exchange: 'exchange.name',
    prefetch: 1,
    durable: true
  };

  var consumer = function(message, done, fail){
    some.service(message, function(err, res){
      if(err) return fail();    
      done();
    });
  };

  rabbit.consume(options, consumer, function(err){
    callback(err);
  });
```

## Destroy
Destory will unbind/purge the queue from the given exchange.
```javascript
  var queue    = "queue.name";
  var exchange = "exchange.name";

  rabbit.destory(queue, exchange, function(err){
    if(!err) log.info("Successfully unbound queue: [" + queue + "]");
    callback(err);
  });
 ```
