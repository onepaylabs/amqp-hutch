# AMQP:Hutch
[amqplib](https://www.npmjs.com/package/amqplib) wrapper for easy setup and initialization.

## Setup
Configuration and Events for AMQP Hutch.
```javascript
var AMQPHutch = require('amqp-hutch');

var hutch = new AMQPHutch();

hutch.initialise({
  connectionString: 'amqps://user:password@host:port/uri?heartbeat=3',
  retryWait:        1000
});

hutch.on('ready', function() {
  console.log('Established RabbitMQ connection');
});

hutch.on('close', function(err) {
  console.log(err.message + 'RabbitMQ closing connection');
});

hutch.on('error', function(err) {
  console.log(err.message + 'RabbitMQ connection error');
});

module.exports = hutch;
```

## Publish
```javascript
var message = {"Message": "Hello"};

var options = {
  exchange: {
    durable: true,
    confirm: true,
    autoDelete: false,
    type: 'topic',
    name: 'example.excahange'
  },
  publish: {
    persistent: true,
    contentType: 'application/json',
    expiration: 86400000,
    timestamp: Math.floor(Date.now() / 1000)
  }
};

hutch.publish(options, message, function(err, res) {
  console.log(res);
});
```


## Publish to Exchange
```javascript
var message = {"Message": "Hello"};

var publishOptions = {
  exchange: {
    durable: true,
    confirm: true,
    autoDelete: false
  },
  publish: {
    persistent: true,
    contentType: 'application/json',
    expiration: 86400000,
    timestamp: Math.floor(Date.now() / 1000)
  }
};

hutch.publishToExchange('exchange.name', 'topic', publishOptions, message, function(err, res) {
  console.log(res);
});
```

## Consume
Consume creates a queue bound to a new channel.
```javascript

  var options = {
    exchange: {
      name: 'exchange.name',
      type: 'topic'
    },
    queue: {
      name: 'queue.name',
      prefetch: 1,
      durable:  true
    }
  };

  var consumer = function(message, done, fail) {
    some.service(message, function(err, res) {
      if(err) return fail();    
      done();
    });
  };

  hutch.consume(options, consumer, function(err) {
    console.log("Successfully setup consumer for queue: [" + options.queue + "]");
  });
```

## Destroy
Destroy will unbind/purge the queue from the given exchange.
```javascript
  var queue    = "queue.name";
  var exchange = "exchange.name";

  hutch.destroy(queue, exchange, function(err) {
    console.log("Successfully unbound queue: [" + queue + "]");
  });
 ```

## Monitoring

amqp-hutch offers two properties which may come in helpful for monitoring.

### status
The status property can contain two values CONNECTED or DISCONNECTED.

### configuration
The configuration object contains the original configuration passed to amqp-hutch.


```javascript
  if (hutch.status === 'CONNECTED'){
    console.log(hutch.configuration.connectionString)
  }
 ```

## Error Handing
Errors triggered from the underlinging service calls with RabbitMQ can be caught by listening for events.

```javascript
hutch.on('error', function(err) {
  console.log(err);
});
```

If the service is invoked prior to a connection been established the service will return a 'AMQPConnectionError' 