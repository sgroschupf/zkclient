CHANGELOG
=====

ZkClient 0.5 (Apr, 2015)
---------------
- Upgrade to zookeeper 3.4.3 (from 3.3.1)
- Added support for multiops support
- Add an option to ZkClient to specify operation retry timeout
- Support for ACLs
- #25: fail retryUntilConnected actions with clear exception in case client gets closed


ZkClient 0.4 (Oct, 2013)
---------------
- Support for handling SessionEstablishmentErrors