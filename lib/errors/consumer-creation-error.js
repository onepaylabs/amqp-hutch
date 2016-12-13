'use strict';

module.exports = require('custom-error-generator')('ConsumerCreationError', null, function(message) {
  this.message = message;
});
