'use strict';

module.exports = require('custom-error-generator')('ChannelAlreadyExists', null, function(message) {
  this.message = message;
});
