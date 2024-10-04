# Release History

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

______________________________________________________________________

<!-- Start TOC -->

**Table of Contents**

- [Release History](#release-history)
  - [\[Unreleased\]](#unreleased)
  - [\[1.4.0 - 2024-10-04\]](#140---2024-10-04)
  - [\[1.3.3 - 2024-05-08\]](#133---2024-05-08)
  - [\[1.3.2 - 2023-05-31\]](#132---2023-05-31)
  - [\[1.3.1 - 2023-03-10\]](#131---2023-03-10)
  - [\[1.3.0 - 2023-01-04\]](#130---2023-01-04)
  - [\[1.2.0 - 2022-12-01\]](#120---2022-12-01)
  - [\[1.1.3 - 2022-09-14\]](#113---2022-09-14)
  - [\[1.1.2 - 2022-09-06\]](#112---2022-09-06)
  - [\[1.1.1 - 2022-08-31\]](#111---2022-08-31)
  - [\[1.1.0 - 2022-08-17\]](#110---2022-08-17)
  - [\[1.0.0 - 2022-07-26\]](#100---2022-07-26)

______________________________________________________________________

<!-- END TOC -->


## \[Unreleased\]

- Added

- Changed

- Deprecated

- Removed

- Fixed


## \[1.4.0 - 2024-10-04\]

- Added
  - Cache transactions


## \[1.3.3 - 2024-05-08\]

- Added

- Changed
  - all references to `mjolnir` are now `strmbrkr`

- Deprecated

- Removed

- Fixed


## \[1.3.2 - 2023-05-31\]

- Added

- Changed

- Deprecated

- Removed

- Fixed
  - `QueueManager.stopHandling` to block until threads are joined


## \[1.3.1 - 2023-03-10\]

- Added
  - `KeyValueStore.submitTransaction()`  to allow for extended / custom transactions

- Changed
  - standard transaction methods of `KeyValueStore` refactored to utilize `KeyValueStore.submitTransaction()`

- Deprecated

- Removed

- Fixed


## \[1.3.0 - 2023-01-04\]

- Added
  - `FlushTransaction` and `KeyValueStore.flush()` for clearing the KVS easily (see #22 & !10)

- Changed

- Deprecated

- Removed

- Fixed
  - usage of `mkdir()` to `makedirs()` to fix race condition when running from clean directory (see #20 & !9)


## \[1.2.0 - 2022-12-01\]

- Added
  - Implement a "Pipeline Status Report" debugging solution to track the status of jobs submitted to mjolnir
  - Utilize TCP sockets on the zmq backend
  - Add a socket to mjolnir network for sending processed jobs back to the QueueManager
  - Implement a configuration module
  - Implement safe_run_tests script that runs each test in its own process

- Changed
  - Refactor how KeyValueStore server is instantiated and when client connections are made
  - Refactor how mjolnir "instance ID" is tracked
  - Bump `setuptools` version
  - Change `zmq` dependency to `pyzmq` dependency

- Deprecated

- Removed

- Fixed
  - Issue where non-thread safe usage of 'frontend' socket was causing messages to be dropped


## \[1.1.3 - 2022-09-14\]

- Added
  - argument to `WorkerManager.__init__()` to allow the watchdog termination time to be runtime configurable

- Changed

- Deprecated

- Removed

- Fixed


## \[1.1.2 - 2022-09-06\]

- Added

- Changed
  - what gets logged in broker thread and key value store transactions

- Deprecated

- Removed
  - logging calls from worker loop

- Fixed


## \[1.1.1 - 2022-08-31\]

- Added
  - dependency on `coverage` library

- Changed
  - Refactored `WorkerManager.__init__()` argument `proc_count` default and validation
  - The `mjolnir.constants` module has been refactored to contain the `EndpointSpecification` class, rather than actual constants

- Deprecated

- Removed
  - dependency on `pytest-cov` library

- Fixed


## \[1.1.0 - 2022-08-17\]

- Added
  - `key_value_store.Transaction` classes
  - `KeyValueStore.popValue()` implementation
  - `JobTimeoutError` exception

- Changed
  - how interactions with `KeyValueStore.Server` are defined (see: `key_value_store.Transaction` classes)
  - default logging level to `INFO`
  - `KeyValueStore.Server` will now be started automatically upon first request (and is daemonic)

- Deprecated

- Removed

- Fixed


## \[1.0.0 - 2022-07-26\]

First release of `mjolnir`.

- Added
  - Re-implementation of `parllel` sub package from `RESONAATE` using ZMQ as infrastructure, rather than Redis.

- Changed

- Deprecated

- Removed

- Fixed

