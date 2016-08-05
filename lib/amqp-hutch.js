'use strict';

var amqp   = require('amqplib/callback_api');
var util   = require('util');

var Emitter = require('events').EventEmitter;

var AMQPConnectionError = require('./errors/amqp-connection-error');

//TODO: Replace with SSL pem file.
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';

var CONNECTED    = "CONNECTED";
var DISCONNECTED = "DISCONNECTED";

// Store Instance of Channel for later lookup.
var channels = {};
var status = DISCONNECTED;

function AMQPHutch() {
}

util.inherits(AMQPHutch, Emitter);


function _isConnectionEstablished(self){
  return self._conn != undefined;
}

function _handleConnectionEstablishError(callback){
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
AMQPHutch.prototype.initialise = function(configuration) {
  this.configuration = configuration;
  this._connectionString = configuration.connectionString;
  this._retryWait = configuration.retryWait;
  this._connect();
};

/**
 * Publish message
 */
AMQPHutch.prototype.publish = function(options, payload, callback) {

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  if(options.exchange){
    this.publishToExchange(options.exchange.name, options.exchange.type, options, payload, callback);
  }
};

/**
 * Publish message to the exchange
 */
AMQPHutch.prototype.publishToExchange = function(exchange, type, options, payload, callback) {

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  options = options || { exchange: {}, publish: {} };

  this._conn.createConfirmChannel(function(err, channel) {
    channel.assertExchange(exchange, type, options.exchange, function(err, ex) {
      if (err) return callback(err);

      channel.on('error', function(err) {
        callback(err);
      });

      channel.publish(ex.exchange, '', new Buffer(JSON.stringify(payload)), options.publish);

      channel.waitForConfirms(function(err) {
        channel.close();
        callback(err);
      });
    });
  });
};


/**
* Consume Queue to Exchange wrapper.
*/
AMQPHutch.prototype.consume = function(options, consumer, callback) {

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  var self = this;

  this._conn.createChannel(function(err, channel) {
    if (err) return callback(err);

    var opts = {
      durable:           options.exchange.durable,
      internal:          options.exchange.durable,
      autoDelete:        options.exchange.durable,
      alternateExchange: options.exchange.durable,
      arguments:         options.exchange.durable
    };

    channel.assertExchange(options.exchange.name, options.exchange.type, opts, function(err, ok) {
      if (err) return callback(err);

      var opts = {
        durable:    options.queue.durable,
        autoDelete: options.queue.autoDelete
      };

      channel.assertQueue(options.queue.name, opts, function(err, ok) {
        if (err) return callback(err);

        channel.bindQueue(ok.queue, options.exchange.name, '');
        channel.prefetch(options.queue.prefetch);

        // Store the channel instance
        channels[ok.queue] = channel;

        channel.on('close', function() {
          delete channels[ok.queue];
        });

        channel.consume(ok.queue, function(message) {

          var done = function() {
            try {
              channel.ack(message);
            } catch(e) {
              // This will be thrown if the channel is closing.
            }
          };

          var fail = function() {
            setTimeout(function() {

              try {
                channel.nack(message);
              } catch(e) {
                // This will be thrown if the channel is closing.
              }
            }, self._retryWait);
          };

          consumer(message, done, fail);
        });

        callback();
      }, {noAck: false});
    });
  });
};

AMQPHutch.prototype.destroy = function(queue, exchange, callback) {

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  // Load channel instance
  this.get(queue, function(err, channel) {
    if (err) return callback(err);

    channel.unbindQueue(queue, exchange, '', {}, function(err, ok) {
      if (err) return callback(err);

      channel.purgeQueue(queue, function(err) {
        if (err) return callback(err);

        channel.close(function(err) {
          callback(err);
        });
      });
    });
  });
};

AMQPHutch.prototype.get = function(queue, callback) {

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  // Load channel instance
  var channel = channels[queue];
  if (channel) return callback(null, channel);

  // If we dont have one create a new one
  this._conn.createChannel(function(err, channel) {
    if (err) return callback(err);
    return callback(err, channel);
  });
};

/**
* close consumer.
*/
AMQPHutch.prototype.close = function(queue, callback) {

  if (!_isConnectionEstablished(this)) return _handleConnectionEstablishError(callback);

  // Load channel instance
  var channel = channels[queue];
  if (!channel) return callback();

  channel.checkQueue(queue, function(err) {
    if (err) return callback(err);

    channel.close(function(err) {
      callback(err);
    });
  });
};

/**
* Initialise Connection
*/
AMQPHutch.prototype._connect = function() {

  var self = this;

  var retry = function() {
    setTimeout(function() {
      self._connect();
    }, self._retryWait);
  };

  // Establish RabbisMQ Connection
  amqp.connect(self._connectionString, function(err, conn) {

    if (err) {
      self.emit('error', err);
      return retry();
    }

    conn.on("error", function(err) {
      self.emit('error', err);
    });

    conn.on("close", function(err) {
      self.emit('close', err);
      self.status = DISCONNECTED;
      return retry();
    });

    self.status = CONNECTED;
    self._conn = conn;
    self.emit('ready', conn);
  });
};

module.exports = AMQPHutch;
