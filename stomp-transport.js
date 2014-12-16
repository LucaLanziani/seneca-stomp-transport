/* Copyright (c) 2014 Luca Lanziani, MIT License */
/*jslint node: true */
"use strict";

var util = require('util');

var _     = require('underscore');
var Stomp = require('stomp-client');

var CHANNEL_RES = "/queue/%s_res";
var CHANNEL_ACT = "/queue/%s_act";

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

  // Legacy patterns
  seneca.add({role:'transport', hook:'listen', type:'queue'}, hook_listen_stomp);
  seneca.add({role:'transport', hook:'client', type:'queue'}, hook_client_stomp);


  function hook_listen_stomp(args, done) {
    var seneca         = this;
    var type           = args.type;
    var listen_options = seneca.util.clean(_.extend({}, options[type], args));

    function listen_topic(topic) {
      var stomp_in  = make_stomp_client(listen_options);
      var stomp_out = make_stomp_client(listen_options);

      var channel_in = util.format(CHANNEL_ACT, topic);
      var channel_out = util.format(CHANNEL_RES, topic);

      stomp_in.on('connect', function () {
        seneca.log.info('listen', 'subscribe', channel_in, seneca);
        stomp_in.subscribe(channel_in, function (msgstr, headers) {
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
        connect_stomp(stomp_in, 'listen_in');
      });

      connect_stomp(stomp_out, 'listen_out');

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
      var stomp_in  = make_stomp_client(client_options);
      var stomp_out = make_stomp_client(client_options);

      var channel_in = util.format(CHANNEL_RES, topic);
      var channel_out = util.format(CHANNEL_ACT, topic);
      var client;

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

      connect_stomp(stomp_in, 'client_in');
      connect_stomp(stomp_out, 'client_out');
    }
  }

  function make_stomp_client(listen_options) {
    return new Stomp(listen_options.host, listen_options.port);
  }

  function connect_stomp(client, identifier) {
    client
      .on('error', function (error) {
        seneca.log.error('transport', 'stomp', identifier, error.message, error.details);
      })
      .connect();

    seneca.add('role:seneca,cmd:close', function (close_args, done) {
      var closer = this;

      client.disconnect();
      closer.prior(close_args, done);
    });

  }

  return {
    name: plugin,
  };
};
