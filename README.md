# Axon Server Connector for Java

[![Maven Central](https://img.shields.io/maven-central/v/io.axoniq/axonserver-connector-java)](https://central.sonatype.com/artifact/io.axoniq/axonserver-connector-java)
![Build Status](https://github.com/AxonIQ/axonserver-connector-java/workflows/AxonServer%20Connector%20Java/badge.svg?branch=master)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=AxonIQ_axonserver-connector-java&metric=alert_status&token=7f2878de3251f0b89dd985bd2fa6dff72c0a7697)](https://sonarcloud.io/dashboard?id=AxonIQ_axonserver-connector-java)

The Axon Server Connector for Java serves the purpose of connecting JVM based applications
to [Axon Server](https://axoniq.io/product-overview/axon-server).

As such it aims to provide a clean and clear solution that ties into
the [Axon Server API](https://github.com/AxonIQ/axon-server-api), which is written
in [ProtoBuf](https://developers.google.com/protocol-buffers). It would thus allow a straightforward connection with
Axon Server, without necessarily using [Axoniq Framework](https://github.com/AxonIQ/AxonFramework).

## About this repository

This repository contains the source for the Axon Server Connector for Java, published under a commercial,
source-available license. We publish it here for transparency, so you can inspect what you're running.

This is not an open-source project in the OSS sense:

- This repository does not accept external pull requests, and we do not maintain a public contribution process for it.
- Being able to read the source does not grant open-source usage rights. Under the license terms, you may modify the
  code for internal use only. You may not distribute the original or modified versions of this software, and you may not
  distribute the source code or any derivative works to third parties.
- If you're looking to contribute code to the Axon ecosystem,
  see [AxonIQ/AxonFramework](https://github.com/AxonIQ/AxonFramework), which remains our open-source project and does
  accept contributions.

The [LICENSE.txt](./LICENSE.txt) contains a short reference that the Axon Server Connector for Java is licensed under
the [Axoniq Terms of Service](https://www.axoniq.io/legal/terms-of-service) for the controlling legal terms. Questions
about licensing, permitted use, or usage rights should go to your Axoniq account contact, not GitHub issues.

## Getting started

Numerous resources can help you on your journey in using the connector.
Let's take a look at some of these:

* The [reference guide](https://docs.axoniq.io) explains all of the components maintained within the Axoniq ecosystem,
  including Axon Server and its connector.
* We have our very own [academy](https://academy.axoniq.io/)!
  The introductory courses are free, followed by more in-depth (paid) courses.
* If the guide doesn't help, our [forum](https://discuss.axoniq.io/) provides a place to ask questions you have during
  development.

## Receiving help

Are you having trouble using this connector?
Know that we want to help you out the best we can!
There are a couple of things to consider when you're traversing anything Axon:

* Checking the [reference guide](https://docs.axoniq.io) should be your first stop.
* When the reference guide does not cover your predicament, we would greatly appreciate it if you could file
  a [documentation issue](https://github.com/AxonIQ/axonserver-connector-java/issues) for it.
* Our [forum](https://discuss.axoniq.io/) provides a space to communicate with the Axon community to help you out.
  Axoniq developers will help you out on a best-effort basis.
  And if you know how to help someone else, we greatly appreciate your contributions!
* We also monitor Stack Overflow for any question tagged with [**axon
  **](https://stackoverflow.com/questions/tagged/axon).
  Similarly to the forum, Axoniq developers help out on a best-effort basis.

## Reporting bugs

Found a bug? Open an issue here with:

- Steps to reproduce
- The connector version
- Relevant logs

We read every issue. Response times follow your support tier, not GitHub's issue queue.

## Feature requests

Feel free to open an issue tagged `enhancement`. We track these, but roadmap decisions are made internally and we can't
commit to timelines here.

## Reporting security issues

Found a security vulnerability? Do not open a public issue.
Email [security-disclosure@axoniq.io](mailto:security-disclosure@axoniq.io) directly so we can assess and patch before
disclosure.

## Code of conduct

Participation in this repository (issues, discussions, etc.) is governed by our [Code of Conduct](./CODE_OF_CONDUCT.md).

---

Copyright (c) Axoniq B.V. All rights reserved. Licensed under
the [Axoniq Terms of Service](https://www.axoniq.io/legal/terms-of-service).
