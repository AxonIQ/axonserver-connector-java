# AxonServer Connector for Java 
[![Build Status](https://api.travis-ci.org/AxonIQ/axonserver-connector-java.svg?branch=master)](https://travis-ci.org/github/AxonIQ/axonserver-connector-java)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.axoniq/axonserver-connector-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.axoniq/axonserver-connector-java)

The AxonServer Connector for Java serves the purpose of connecting JVM based application to [AxonServer](https://axoniq.io/product-overview/axon-server).

As such it aims to provide a clean and clear solution that ties into the [AxonServer API](https://github.com/AxonIQ/axon-server-api), which is written in [ProtoBuf](https://developers.google.com/protocol-buffers).
It would thus allow a straightforward connection with AxonServer, without necessarily using [Axon Framework](https://github.com/AxonFramework/AxonFramework).
On top of this, it can be used as a clear starting point to build your own connector to AxonServer in your preferred language. 
  
For more information on anything Axon, please visit our website, [http://axoniq.io](http://axoniq.io).

## Receiving help

Are you having trouble using this connector, or implementing your own language specific version of it? 
We'd like to help you out the best we can!
There are a couple of things to consider when you're traversing anything Axon:

* There is a [public mailing list](https://groups.google.com/forum/#!forum/axonframework) to support you in the case 
 the reference guide did not sufficiently answer your question.
* Next to the mailing list we also monitor Stack Overflow for any questions which are tagged with `axon`.

## Feature requests and issue reporting

We use GitHub's [issue tracking system](https://github.com/AxonIQ/axonserver-connector-java/issues) for new feature request, enhancements and bugs. 
Prior to filing an issue, please verify that it's not already reported by someone else.

When filing bugs:
* A description of your setup and what's happening helps us to figure out what the issue might be
* Do not forget to provide the version you're using
* If possible, share a stack trace, using the Markdown semantic ```

When filing features:
* A description of the envisioned addition or enhancement should be provided
* (Pseudo-)Code snippets showing what it might look like help us understand your suggestion better 
* If you have any thoughts on where to plug this into the framework, that would be very helpful too
* Lastly, we value contributions to the framework highly. So please provide a Pull Request as well!
 