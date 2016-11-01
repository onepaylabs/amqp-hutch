var mocha   = require('mocha');
var should  = require('chai').should();

var AMQPHutch = require('..');

describe('Hutch', function() {

  it('should bind and consumer a message', function(complete) {

    var hutch = new AMQPHutch();

    hutch.initialise({
      connectionString: 'amqp://localhost',
      retryWait:        100
    });

    hutch.on('ready', function() {

      var consumer = function(message, done, fail) {
        JSON.parse(message.content).should.equal('Example Message!');
        done();
        complete();
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
        hutch.publish(options, "Example Message!", function(err, res){});
      });
    });
  });

  it('should error when not connected', function(complete) {

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

    hutch.on('ready', function() {
      hutch.publish(options, "Example Message!", function(err){});
    });
      
    hutch.once('error', function(err) {
      complete();
    });
  });

  it('should destroy a queue', function(complete) {

    var hutch = new AMQPHutch();

    hutch.initialise({
      connectionString: 'amqp://localhost',
      retryWait:        100
    });

    hutch.on('ready', function() {

      var options = {
        exchange: {
          name: 'example.exchange.2',
          type: 'topic'
        },
        queue: {
          name: 'example.queue.1',
          prefetch: 1,
          durable:  true
        },
        publish: {
          persistent: true,
          expiration: 86400000
        }
      };

      var consumer = function(message, done, fail) {
        hutch.destroy(options.queue.name, options.exchange.name, function(err){
          should.not.exist(err);
          complete();
        });
      };

      hutch.consume(options, consumer, function(err) {
        hutch.publish(options, "Example Message!", function(err, res){});
      });
    });
  });
});
