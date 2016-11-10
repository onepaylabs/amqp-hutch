'use strict';

module.exports = require('custom-error-generator')('NoChannelError', null, function(message) {
  this.message = message;
});
