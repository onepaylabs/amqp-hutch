'use strict';

module.exports = require('custom-error-generator')('ChannelClosed', null, function(message) {
  this.message = message;
});
