/* Copyright (c) 2014 Luca Lanziani */

// mocha stomp-transport.test.js

var test = require('seneca-transport-test');


describe('stomp-transport', function() {

  it('happy-any', function( fin ) {
    test.foo_test( 'stomp-transport', require, fin, 'stomp', -6379 );
  });

  it('happy-pin', function( fin ) {
    test.foo_pintest( 'stomp-transport', require, fin, 'stomp', -6379 );
  });

});
