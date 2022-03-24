# PGRanger
Client side sharding library for Postgresql in Golang. Application processes can load sharding information in memory and talk to
a group of postgres instances with no middleman. Application can assume there is one single db instance in the
transaction code. The sharding key space can be custom divided by user into micro-partition which will be moved as a
unit, e.g. we can divide the sharding key of user_id into 256 ranges upfront, located on 16 db hosts, and if we need to scale
up, we can move some ranges to new db hosts.

Full example in example/userprofile.

Advantages:
1. Compared with Vitess, which does not have Postgres support, pgranger eliminates the middle man like
   VTGate, VTTablet, and thus reduces latency and failure points. It is also simpler to understand and maintain.
2. Compared with CockroachDB, pgranger is faster on what it can do, namely the single shard data access. This is because
   pgranger is essential postgres underhood, which is faster than CockroachDB when not overwhelmed.
3. Compared with application level sharding, pgranger is strictly better because it does not leak sharding information outside of application.

In summary, if your application needs a simple and scalable postgres solution, and the traffic pattern can be formulated into single shard data access, then pgranger could potentially help.

Downside:
1. No data migration support. Will add as a next step.
2. No distributed transactions. Investigating as a next step. Because sharding information is on application process, a custom version could potentially be built.

Would love any discussion, contribution or help on this!! Can be reached at olddaves at gmail.com
