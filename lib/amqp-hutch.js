'use strict';

var amqp   = require('amqplib/callback_api');
var util   = require('util');

var Emitter = require('events').EventEmitter;

//TODO: Replace with SSL pem file.
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';

// Store Instance of Channel for later lookup.
var channels = {};

function AMQPHutch() {
};

util.inherits(AMQPHutch, Emitter);

/**
* Initialise AMQPHutch Connection
*/
AMQPHutch.prototype.initialise = function(configuration) {
  this._connectionString = configuration.connectionString;
  this._retryWait = configuration.retryWait;
  this._connect();
};

/**
* Consume Queue to Exchange wrapper.
*/
AMQPHutch.prototype.consume = function(options, consumer, callback){
  var self = this;

  this._conn.createChannel(function(err, channel){
    if(err) return callback(err);

    channel.checkExchange(options.exchange, function(err){
      if(err) return callback(err);

      var opts = {
        durable: options.durable,
        autoDelete: options.autoDelete
      }

      channel.assertQueue(options.queue, opts, function(err){
        if(err) return callback(err);

        channel.bindQueue(options.queue, options.exchange, '');
        channel.prefetch(options.prefetch);

        // Store the channel instance
        channels[opts.queue] = channel;

        channel.consume(options.queue, function(message){

          var done = function(){
            channel.ack(message);
          };

          var fail = function(){
            setTimeout(function(){
              channel.nack(message);
            }, self._retryWait);
          };

          consumer(message, done, fail);
        });

        callback();
      }, {noAck: false});
    });
  });
};

AMQPHutch.prototype.destory = function(queue, exchange, callback){

  // Load channel instance
  var channel = channels[queue];

  channel.checkQueue(queue, function(err){
    if(err) return callback(err);

    channel.unbindQueue(queue, exchange, '', {}, function(err, ok){
      if(err) return callback(err);

      channel.purgeQueue(queue, function(err){
        if(err) return callback(err);

        channel.close(function(err){
          callback(err);
        });
      });
    });
  });
};

/**
* Initialise Connection
*/
AMQPHutch.prototype._connect = function() {
  var self = this;

  var retry = function(){
    setTimeout(function(){
      self._connect();
    }, self._retryWait);
  };

  // Establish RabbisMQ Connection
  amqp.connect(self._connectionString, function(err, conn){

    if (err) {
      self.emit('error', err);
      return retry();
    }

    conn.on("error", function(err){
      self.emit('error', err);
    });

    conn.on("close", function(err){
      self.emit('close', err);
      return retry();
    });

    self._conn = conn;
    self.emit('ready', conn);
  });
};

var hutch = module.exports = new AMQPHutch();
