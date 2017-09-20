'use strict';

var amqp  = require('amqplib/callback_api');
var async = require('async');
var util  = require('util');

var Emitter = require('events').EventEmitter;

var AMQPConnectionError   = require('./errors/amqp-connection-error');
var NoChannelError        = require('./errors/no-channel-error');
var ChannelAlreadyExists  = require('./errors/channel-already-exists');
var ChannelClosed         = require('./errors/channel-closed');
var ConsumerCreationError = require('./errors/consumer-creation-error');

//TODO: Replace with SSL pem file.
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';

var CONNECTED    = "CONNECTED";
var DISCONNECTED = "DISCONNECTED";

var PUBLISH_CHANNEL = "publish.channel";

function AMQPHutch() {
  this.channels = {};
  this.closed   = {};
  this.status   = DISCONNECTED;
}

util.inherits(AMQPHutch, Emitter);

AMQPHutch.prototype._isConnectionEstablished = function() {
  return this._conn != undefined && this.status === CONNECTED;
};

AMQPHutch.prototype._handleConnectionEstablishError = function(callback) {
  return callback(new AMQPConnectionError('Could not Establish a Connection to RabbitMQ Server'));
};

/**
 * Configuration Accessor
 *
 * @type {undefined}
 */
AMQPHutch.prototype.configuration = null;

/**
 * Initialise AMQPHutch Connection
 */
AMQPHutch.prototype.initialise = function (configuration) {
  this.configuration     = configuration;
  this._connectionString = configuration.connectionString;
  this._retryWait        = configuration.retryWait;
  this._connect();
};

/**
 * Publish message
 */
AMQPHutch.prototype.publish = function (options, payload, callback) {

  if (!this._isConnectionEstablished()) return this._handleConnectionEstablishError(callback);

  if (options.exchange) {
    this.publishToExchange(options.exchange.name, options.exchange.type, options, payload, callback);
  }
};

/**
 * Publish message to the exchange
 */
AMQPHutch.prototype.publishToExchange = function (exchange, type, options, payload, callback) {
  var self    = this;
  var channel = this._getChannelByExchange(exchange);

  if (!this._isConnectionEstablished()) return this._handleConnectionEstablishError(callback);

  options = options || {exchange: {}, publish: {}};

  // Load Existing Channel for publishing to this exchange.
  if (channel){
    channel.publish(exchange, options.exchange.routingKey || '', new Buffer(JSON.stringify(payload)), options.publish);
    channel.waitForConfirms(function (err) {
      callback(err);
    });
  }
  // Create new channel for publishing.
  else {
    this._conn.createConfirmChannel(function (err, channel) {
      if (err) return callback(err);

      channel.on('error', function (err) {
        // This will be handled by the 'close' event.npm
      });

      channel.on('close', function () {
        delete self.channels[PUBLISH_CHANNEL];
      });

      channel.assertExchange(exchange, type, options.exchange, function (err, ex) {
        if (err) return callback(err);

        channel.exchange = exchange;
        self.channels[PUBLISH_CHANNEL] = channel;

        channel.publish(ex.exchange, options.exchange.routingKey || '', new Buffer(JSON.stringify(payload)), options.publish);
        channel.waitForConfirms(function (err) {
          callback(err);
        });
      });
    });
  }
};

/**
 * Consume Queue to Exchange wrapper.
 */
AMQPHutch.prototype.consume = function (options, consumer, callback) {

  var self = this;

  if (!self._isConnectionEstablished(self)) return self._handleConnectionEstablishError(callback);
  if (self._getChannelByQueue(options.queue.name)) return callback(new ChannelAlreadyExists("Channel already exists for " + options.queue.name));
  if (self.closed[options.queue.name]) delete self.closed[options.queue.name];

  function subscribe(){
    if ( self.closed[options.queue.name]) return callback(new ChannelClosed("Channel has already been closed"));

    var createChannel = function(next){
        try {
          self._conn.createChannel(function (err, channel) {
            next(err, channel);
          });
        }
        catch(e){
          /* Random error thrown during the operation of creating a new channel. This could be trigger due to connectivity issues or a unknown uncaught error */
          next(new ChannelClosed("Channel closed unexpectedly"));
        }
      };

    var checkExchange = function(channel, next){
        channel.on('error', function () {
          /* Silently handle channel error as we are only interested in the channel closure. */
        });

        channel.assertExchange(options.exchange.name, options.exchange.type, options.exchange, function (err, ok) {
          next(err, channel);
        });
      };

    var checkQueue = function(channel, next){
      var opts = { noAck: false };

      channel.assertQueue(options.queue.name, options.queue, function (err, ok) {
          next(err, ok, channel);
        }, opts);
      };

    var bindAndConsume = function(ok, channel, next) {
      var opts = { exclusive: options.exclusive };

      channel.bindQueue(ok.queue, options.exchange.name, options.routingKey || '#');
      channel.prefetch(options.queue.prefetch);
      channel.queue = ok.queue;
      self.channels[channel.ch] = channel;

      channel.consume(ok.queue, function (message) {

        // Protect against null messages being consumed on shutdown.
        if (message == null) return;

        var done = function () {
          try { channel.ack(message); }
          catch (e) {
            /* Random error thrown during the operation of a message ack. This could be trigger due to connectivity issues or a unknown uncaught error */
            console.log("Error during message Ack");
          }
        };

        var fail = function () {
          setTimeout(function () {
            try { channel.nack(message); }
            catch (e) {
              /* Random error thrown during the operation of a message nack. This could be trigger due to connectivity issues or a unknown uncaught error */
              console.log("Error during message Nack");
            }
          }, self._retryWait);
        };

        // If the skip next option has been set remove the next message and remove the temp parameter.
        if(options.skipNext){
          delete options.skipNext;
          return done();
        }

        consumer(message, done, fail);
      }, opts, function (err, ok) {

        if (!err){
          channel.on('close', function () {
            self.emit('channelClosed', self.channels[channel.ch].queue);
            delete self.channels[channel.ch];
          });

          next();
        }
        else if (err != undefined && err.message != undefined && err.message.includes('403')) {
          // Delete skipNext when not the active consumer.
          delete options.skipNext;
          delete self.channels[channel.ch];
          // Retry with Random Timeout for Exclusive consumer.
          setTimeout(subscribe, Math.floor(Math.random() * 4000) + 2000);
        }
        else {
          next(err);
        }
      });
    };

    async.waterfall([ createChannel, checkExchange, checkQueue, bindAndConsume], function(err){
      if(!err) return callback();

      self.close(options.queue.name, function(){
        callback(new ConsumerCreationError("Unable to create a consumer: " + err.message));
      });
    });
  }

  subscribe();
};

AMQPHutch.prototype.destroy = function (queue, exchange, callback) {

  if (!this._isConnectionEstablished(this)) return this._handleConnectionEstablishError(callback);

  // Add the queue to the closed object.
  this.closed[queue] = true;

  this.get(queue, function(err, channel){

    if (err) return callback(new NoChannelError('Could not create channel for destroy'));

    channel.unbindQueue(queue, exchange, '', {}, function (err, ok) {
      if (err) return callback(err);

      channel.deleteQueue(queue, {}, function (err, ok) {
        if (err) return callback(err);

        channel.close(function (err) {
          callback(err);
        });
      });
    });
  });
};

AMQPHutch.prototype.get = function (queue, callback) {

  if (!this._isConnectionEstablished(this)) return this._handleConnectionEstablishError(callback);

  // Load channel instance
  var channel = this._getChannelByQueue(queue);
  if (channel) return callback(null, channel);

  // If we dont have one create a new one
  this._conn.createChannel(function (err, channel) {
    if (err) return callback(err);

    channel.on('error', function (err) {
      // This will be handled by the 'close' event.
    });

    return callback(err, channel);
  });
};

/**
 * close consumer.
 */
AMQPHutch.prototype.close = function (queue, callback) {

  if (!this._isConnectionEstablished(this)) return this._handleConnectionEstablishError(callback);

  // Add the queue to the closed object.
  this.closed[queue] = true;

  // Load channel instance
  var channel = this._getChannelByQueue(queue);
  if (!channel) return callback(new NoChannelError('Could not close channel that does not exist'));

  channel.checkQueue(queue, function (err) {
    if (err) return callback(err);

    channel.close(function (err) {
      callback(err);
    });
  });
};

AMQPHutch.prototype.isConnected = function () {
  return this.status === CONNECTED;
};

/**
 * Initialise Connection
 */
AMQPHutch.prototype._connect = function () {

  var self = this;

  var retry = function () {
    setTimeout(function () {
      self._connect();
    }, self._retryWait);
  };

  // Establish RabbisMQ Connection
  amqp.connect(self._connectionString, function (err, conn) {

    if (err) {
      self.emit('error', err);
      return retry();
    }

    conn.on("error", function (err) {
      self.emit('error', err);
    });

    conn.once("close", function (err) {
      self.emit('close', err);
      self.status = DISCONNECTED;
      retry();
    });

    self.status = CONNECTED;
    self._conn = conn;
    self.emit('ready', conn);
  });
};

AMQPHutch.prototype._getChannelByQueue = function(queue){
  for(var id in this.channels) {

    // Ignore the explicit publish channel.
    if(id !== PUBLISH_CHANNEL && this.channels[id].queue === queue) return this.channels[id];
  }
};

AMQPHutch.prototype._getChannelByExchange = function(exchange){
  for(var id in this.channels) {
    if(this.channels[id].exchange === exchange) return this.channels[id];
  }
};

module.exports = AMQPHutch;
module.exports.AMQPConnectionError   = AMQPConnectionError;
module.exports.NoChannelError        = NoChannelError;
module.exports.ChannelAlreadyExists  = ChannelAlreadyExists;
module.exports.ChannelClosed         = ChannelClosed;
module.exports.ConsumerCreationError = ConsumerCreationError;
module.exports.CONNECTED             = CONNECTED;
module.exports.DISCONNECTED          = DISCONNECTED;
