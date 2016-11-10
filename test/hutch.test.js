var mocha   = require('mocha');
var should  = require('chai').should();

var AMQPHutch = require('..');

describe('Hutch', function() {

  it('isConnected should return true when hutch is connected', function(complete) {
    var hutch = new AMQPHutch();

    hutch.initialise({
      connectionString: 'amqp://localhost',
      retryWait:        100000
    });

    hutch.on('ready', function(){
      hutch.isConnected().should.equal(true);
      complete();
    })
  });

  it('isConnected should return false when hutch is not connected', function(complete) {
    var hutch = new AMQPHutch();
    hutch.isConnected().should.equal(false);
    complete();
  });

  it('should return a no channel error when attempting to close a channel that doesnt exist', function(complete) {
    var hutch = new AMQPHutch();

    hutch.initialise({
      connectionString: 'amqp://localhost',
      retryWait:        100000
    });

    hutch.on('ready', function(){
      hutch.close("not/going/to/exist", function(err){
        err.name.should.equal('NoChannelError');
        complete();
      })
    })
  });

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
    this.timeout(5000); // Allow up to 5 seconds for windows

    var hutch = new AMQPHutch();

    hutch.initialise({
      connectionString: 'amqp://bad',
      retryWait:        100000
    });

    hutch.on('error', function(err) {
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

  it('should trigger a closeChannel event with the queue name for the channel', function(complete) {

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
        hutch.close(options.queue.name, function(err){});
      };

      hutch.consume(options, consumer, function(err) {
        hutch.publish(options, "Example Message!", function(err, res){});
      });
    });

    hutch.on('channelClosed', function(queue){
      queue.should.equal('example.queue.1');
      complete();
    });
  });
});
