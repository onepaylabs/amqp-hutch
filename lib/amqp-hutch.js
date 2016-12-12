'use strict';

var amqp  = require('amqplib/callback_api');
var async = require('async');
var util  = require('util');

var Emitter = require('events').EventEmitter;

var AMQPConnectionError  = require('./errors/amqp-connection-error');
var NoChannelError       = require('./errors/no-channel-error');
var ChannelAlreadyExists = require('./errors/channel-already-exists');
var ChannelClosed        = require('./errors/channel-closed');

//TODO: Replace with SSL pem file.
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';

var CONNECTED    = "CONNECTED";
var DISCONNECTED = "DISCONNECTED";

// Store Instance of Channel for later lookup.
var channels = {};
var closed   = {};

var status = DISCONNECTED;

function AMQPHutch() {
}

util.inherits(AMQPHutch, Emitter);


function _isConnectionEstablished(self) {
  return self._conn != undefined && self.status === CONNECTED;
}

function _handleConnectionEstablishError(callback) {
  return callback(new AMQPConnectionError('Could not Establish a Connection to RabbitMQ Server'));
}

/**
 * Exposes a state variable
 *
 * @type {string}
 */
AMQPHutch.prototype.status = status;

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

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  if (options.exchange) {
    this.publishToExchange(options.exchange.name, options.exchange.type, options, payload, callback);
  }
};

/**
 * Publish message to the exchange
 */
AMQPHutch.prototype.publishToExchange = function (exchange, type, options, payload, callback) {

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  options = options || {exchange: {}, publish: {}};

  this._conn.createConfirmChannel(function (err, channel) {
    if (err) return callback(err);

    channel.on('error', function (err) {
      // This will be handled by the 'close' event.npm
    });

    channel.assertExchange(exchange, type, options.exchange, function (err, ex) {
      if (err) return callback(err);

      channel.publish(ex.exchange, '', new Buffer(JSON.stringify(payload)), options.publish);

      channel.waitForConfirms(function (err) {
        channel.close();
        callback(err);
      });
    });
  });
};

/**
 * Consume Queue to Exchange wrapper.
 */
AMQPHutch.prototype.consume = function (options, consumer, callback) {

  var self = this;
  if ( !_isConnectionEstablished(self)) return _handleConnectionEstablishError(callback);
  if ( closed[options.queue.name]) delete closed[options.queue.name];

  function subscribe(){

    // If the Channel is already bound to this queue;
    if (getChannelByQueue(options.queue.name)) return callback(new ChannelAlreadyExists("Channel already exists for " + options.queue.name));
    if (closed[options.queue.name]) return callback(new ChannelClosed("Channel has already been closed"));

    var createChannel = function(next){
        try {
          self._conn.createChannel(function (err, channel) {
            next(err, channel);
          });
        }
        catch(e){
          /* Random error thrown during the operation of creating a new channel. This could be trigger due to connectivity issues or a unknown uncaught error */
          console.log("Error Creating Channel");
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

      channel.on('close', function () {
        self.emit('channelClosed', ok.queue);
        delete channels[channel.ch];
      });

      channel.bindQueue(ok.queue, options.exchange.name, options.routingKey || '#');
      channel.prefetch(options.queue.prefetch);
      channel.queue = ok.queue;
      channels[channel.ch] = channel;

      channel.consume(ok.queue, function (message) {

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

        consumer(message, done, fail);
      }, opts, function (err, ok) {

        // Retry with Random Timeout for Exclusive consumer.
        if (err != undefined && err.message != undefined && err.message.includes('403')) {
          setTimeout(subscribe, Math.floor(Math.random() * 4000) + 2000);
        }
        next(err);
      });
    };

    async.waterfall([ createChannel, checkExchange, checkQueue, bindAndConsume], function(err){
      callback(err);
    });
  }

  subscribe();
};

AMQPHutch.prototype.destroy = function (queue, exchange, callback) {

  // Add the queue to the closed object.
  closed[queue] = true;

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  // Load channel instance
  this.get(queue, function (err, channel) {
    if (err) return callback(err);

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

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  // Load channel instance
  var channel = getChannelByQueue(queue);
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

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  // Add the queue to the closed object.
  closed[queue] = true;

  // Load channel instance
  var channel = getChannelByQueue(queue);
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

/**
 * Get the Channel by Queue Name
 * @param queue
 */
function getChannelByQueue(queue){
  for(var id in channels) {
    if(channels[id].queue === queue) return channels[id];
  }
}


module.exports = AMQPHutch;
module.exports.AMQPConnectionError  = AMQPConnectionError;
module.exports.NoChannelError       = NoChannelError;
module.exports.ChannelAlreadyExists = ChannelAlreadyExists;
module.exports.ChannelClosed        = ChannelClosed;
module.exports.CONNECTED            = CONNECTED;
module.exports.DISCONNECTED         = DISCONNECTED;
