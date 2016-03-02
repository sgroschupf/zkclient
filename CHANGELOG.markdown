CHANGELOG
=====

ZkClient 0.8.1 (Feb,2016)
---------------
-- Update log4j to log4j2

ZkClient 0.8 (???)
---------------



ZkClient 0.7 (Nov 2015)
---------------
- #38: wait on SaslAuthenticated event when SASL is enabled


ZkClient 0.6 (Aug 2015)
---------------
- Adding setAcl and getAcl methods to zkClient so users can setAcls not just during creation but after creation of node as well.
- Upgrade to Zookeeper 3.4.6 (from 3.4.3)


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