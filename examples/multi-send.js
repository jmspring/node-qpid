#!/usr/bin/env node
var Messenger = require('..').proton.Messenger,
    async     = require("async");

var optimist = require('optimist')
    .options('a', { alias : 'address', default : 'amqp://0.0.0.0', } )
    .options('n', { alias : 'count', default : 1 })
    .options('w', { alias : 'windowsize', default : 50})   
    .options('m', { alias : 'message' })
    .demand([ 'm' ]) 
    .usage("$0 -[-a address] [-n count] [-w windowsize] messagee ")
  ;

var address = optimist.argv.address;
var count = optimist.argv.count;
var windowsize = optimist.argv.windowsize;
var message = optimist.argv.message;

var m = new Messenger();
var message = { address: address, body: message };

var resolved = 0;
var errors = 0;
var startTime = new Date().getTime();

function message_callback(err, msg) {
  if(err) {
    errors++;
  }
  resolved++;
  endTime = new Date().getTime();
}

function status_check() {
  if(resolved == count) {
    console.log("Done: %d sent, %d errors, %d ms", resolved, errors, (endTime - startTime));
  } else {
    console.log("Sent: " + resolved + " of " + count);
    setTimeout(status_check, 2500);
  }
}

var i = 0;
var batch = 50;    // number of messages to send in parallel

function send_batch() {
  var queue = [];
  
  function add_message(idx) {
    queue.push(function(callback) {
      m.send(message, function(err, result) {
        message_callback(err, result);
        callback(null);
      })
    });
  }

  for(var j = 0; j < windowsize && i < count; i++, j++) {
    add_message(i);          
  }

  if(queue.length > 0) {        
    async.parallel(queue, function(err, res) {
      if(err) {
        process.exit(1);
      } else {
        send_batch();
      }
    });
  }
}

setTimeout(status_check, 2500);
send_batch();
