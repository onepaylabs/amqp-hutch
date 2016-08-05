'use strict';

module.exports = require('custom-error-generator')('AMQPConnectionError', null, function(message) {
  this.message = message;
});
