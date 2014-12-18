seneca-stomp-transport - a [Seneca](http://senecajs.org) plugin
======================================================

## Seneca Stomp Transport Plugin

Seneca message transport over Stomp, the default configuration does NOT work ActiveMQ.

For any question feel free to contact me at [@_Nss_](http://twitter.com/_Nss_)

Tested on Node v0.10.33, Seneca 0.5.20, [coilmq](https://github.com/Nss/coilmq)



## Test

As reported above the default configuration doesn't work with ActiveMQ.
This is because we want to keep this module as much generic as possible, ActiveMQ forces the channel prefix to be "/queue/", this is not true for other brokers.

To test this package you need a stomp broker that doesn't have constraints on the channel name.

Ex.

In your code directory:

```
git clone https://github.com/Nss/coilmq.git
cd coilmq
PYTHONPATH=. python2 coilmq/start.py -p 61613 --debug
```

Inside the seneca-stomp-transport directory:

```
npm test
```

## Acknowledgements

Sponsored by [nearForm](http://www.nearform.com/)