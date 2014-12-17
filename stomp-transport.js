/* Copyright (c) 2014 Luca Lanziani, MIT License */
"use strict";

var util = require('util');

var _     = require('underscore');
var Stomp = require('stomp-client');

module.exports = function( options ) {
  var seneca = this;
  var plugin = 'stomp-transport';

  var so = seneca.options();

  var ACT_CHANNEL = "/queue/%s_act";
  var RES_CHANNEL = "/queue/%s_res";

  options = seneca.util.deepextend(
    {
      stomp: {
        timeout:     so.timeout ? so.timeout-555 :  22222,
        type:        'stomp',
        host:        'localhost',
        port:        61613,
        act_channel: ACT_CHANNEL,
        res_channel: RES_CHANNEL
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
      var channel_in = util.format(listen_options.act_channel, topic);
      var channel_out = util.format(listen_options.res_channel, topic);

      var stomp_in  = make_stomp_client(listen_options, 'listen_in');
      var stomp_out = make_stomp_client(listen_options, 'listen_out');

      stomp_in.on('connect', function () {
        seneca.log.debug('listen', 'connected', channel_in, seneca);
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
        stomp_in.connect();
      });

      stomp_out.connect();

    }

    tu.listen_topics( seneca, args, listen_options, function(topic) {
      listen_topic(topic);
    });

    seneca.log.debug('listen', 'open', listen_options, seneca);

    done();
  }


  function hook_client_stomp(args, clientdone) {
    var seneca         = this;
    var type           = args.type;
    var client_options = seneca.util.clean(_.extend({}, options[type], args));

    tu.make_client(make_send, client_options, clientdone);

    function make_send(spec, topic, send_done) {
      var channel_in = util.format(client_options.res_channel, topic);
      var channel_out = util.format(client_options.act_channel, topic);

      var stomp_in  = make_stomp_client(client_options, 'client_in');
      var stomp_out = make_stomp_client(client_options, 'client_out');

      stomp_in.on('connect', function (session_id) {
        seneca.log.debug('client', 'connected', channel_in, client_options, seneca);

        stomp_in.subscribe(channel_in, function (msgstr) {
          var input = tu.parseJSON(seneca, 'client-'+type, msgstr);
          seneca.log.debug('client', 'input', input);
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

    }
  }

  function make_stomp_client(options, identifier) {
    var client = new Stomp(options.host, options.port);
    client.on('error', function (error) {
      seneca.log.error('transport', 'stomp', identifier, error.message);
    });

    seneca.add('role:seneca,cmd:close', function (close_args, done) {
      var closer = this;

      client.disconnect();
      closer.prior(close_args, done);
    });
    return client;
  }

  return {
    name: plugin,
  };
};
