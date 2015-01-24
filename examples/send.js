var Messenger = require('..').proton.Messenger;
var crypto = require('crypto');
var async = require('async');

var optimist = require('optimist')
    .options('a', {
        alias : 'address',
        default : 'amqp://0.0.0.0',
    })
    .usage("$0 [-a address] message")
;

var message;
if (optimist.argv._.length < 1) {
  console.log(optimist.help());
} else {
  message = optimist.argv._[optimist.argv._.length-1];
}

var m = new Messenger();

m.on('error', function(err) {
  console.log("Error: " + err.message);
});

var messages = {};
var size = 128;
var count = 10000;
for(var i = 0; i < count; i++) {
  messages[i] = {
    id: i,
    content: new Buffer(crypto.randomBytes(size)).toString('base64')
  };
}
console.log("Message size -- " + messages[0].content.length);

var processed = 0;
function watcher() {
  if(processed == count) {
    console.log("Total time for %d messages:  %d, errors: %d", count, end - start, errors);
  } else {
    console.log("%d of %d processed, errors: %d", processed, count, errors);
    setTimeout(watcher, 5000);
  }
}

setTimeout(watcher, 5000);

var sent = count;
var end;
var start = new Date().getTime();
var errors = 0;
for(var i = 0; i < count; i++) {
  function send_n(n) {
    m.send({ address: optimist.argv.address, body: messages[n].content }, function(err, val) {
      if(!err) {
        end = new Date().getTime();
      } else {
        if(err == "aborted") {
          m.send(val, function(err, val) {
            if(err) {
              errors++;
              console.log("ERR -- " + n + " : " + err);
            } else {
              end = new Date().getTime();
            }
          });
        } else {
          errors++;
          console.log("ERR -- " + err);
        }
      }
      processed++;
    });
  }
  send_n(i);
}
console.log("yup");