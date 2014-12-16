/* Copyright (c) 2014 Luca Lanziani, MIT License */
"use strict";

var util = require('util');

var _     = require('underscore');
var Stomp = require('stomp-client');

module.exports = function( options ) {
  var seneca = this;
  var plugin = 'stomp-transport';

  var so = seneca.options();

  options = seneca.util.deepextend(
    {
      stomp: {
        timeout:  so.timeout ? so.timeout-555 :  22222,
        type:     'stomp',
        host:     'localhost',
        port:     61613
      },
    },
    so.transport,
    options);


  var tu = seneca.export('transport/utils');

  seneca.add({role:'transport', hook:'listen', type:'stomp'}, hook_listen_stomp);
  seneca.add({role:'transport', hook:'client', type:'stomp'}, hook_client_stomp);


  function hook_listen_stomp(args, done) {
    var seneca         = this;
    var type           = args.type;
    var listen_options = seneca.util.clean(_.extend({}, options[type], args));

    function listen_topic(topic) {
      var channel_in = util.format("/queue/%s_act", topic);
      var channel_out = util.format("/queue/%s_res", topic);

      var stomp_in  = new Stomp(listen_options.host, listen_options.port);
      var stomp_out = new Stomp(listen_options.host, listen_options.port);

      stomp_in.on('error', on_error);
      stomp_out.on('error', on_error);

      stomp_in.on('connect', function () {

        stomp_in.subscribe(channel_in, function (msgstr, headers) {
          seneca.log.info('listen', 'subscribe', channel_in, headers, seneca);
          var data = tu.parseJSON(seneca, 'listen-'+type, msgstr);

          tu.handle_request(seneca, data, listen_options, function (out) {
            if (null === out) return;
            var outstr = tu.stringifyJSON(seneca, 'listen-'+type, out);
            stomp_out.publish(channel_out, outstr);
          });
        });
      });

      stomp_out.on('connect', function () {
        seneca.log.debug('listen', 'connect', 'out', topic, seneca);
        stomp_in.connect();
      });

      stomp_out.connect();

    }

    tu.listen_topics( seneca, args, listen_options, function(topic) {
      listen_topic(topic);
    });

    seneca.log.info('listen', 'open', listen_options, seneca);

    done();
  }


  function hook_client_stomp(args, clientdone) {
    var seneca         = this;
    var type           = args.type;
    var client_options = seneca.util.clean(_.extend({}, options[type], args));

    tu.make_client(make_send, client_options, clientdone);

    function make_send(spec, topic, send_done) {
      var channel_in = util.format("/queue/%s_res", topic);
      var channel_out = util.format("/queue/%s_act", topic);

      var stomp_in  = new Stomp(client_options.host, client_options.port);
      var stomp_out = new Stomp(client_options.host, client_options.port);

      stomp_in.on('error', on_error);
      stomp_out.on('error', on_error);

      stomp_in.on('connect', function (session_id) {
        seneca.log.debug('client', 'connected', channel_in, client_options, seneca);
        stomp_in.subscribe(channel_in, function (msgstr) {

          var input = tu.parseJSON(seneca, 'client-'+type, msgstr);
          seneca.log.info('client', 'input', input);
          tu.handle_response(seneca, input, client_options);
        });
      });


      stomp_out.on('connect', function (session_id) {

        send_done( null, function( args, done ) {
          var outmsg = tu.prepare_request(this, args, done);
          var outstr = tu.stringifyJSON(seneca, 'client-'+type, outmsg);

          stomp_out.publish(channel_out, outstr);
        });

      });

      stomp_out.connect();
      stomp_in.connect();

      seneca.add('role:seneca,cmd:close', function (close_args, done) {
        var closer = this;

        stomp_in.disconnect();
        stomp_out.disconnect();
        closer.prior(close_args, done);
      });
    }
  }

  function on_error(error) {
    seneca.log.error('transport', 'stomp', error.message, error.details);
  }

  return {
    name: plugin,
  };
};
