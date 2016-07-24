Helper Scripts for Object Storage Migration From Ceph Rados Gateway to Swift
============================================================================

Check duplicate container name between regions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Before we do migration from RGW to Swift, the first thing we need to check is
if there are containers with same name existing in different regions, which
will lead to customers data loss.

You can use the following command for the check::

    python swift-check-duplicate.py openstack:objectmonitor \
           https://api.cloud.catalyst.net.nz:5000/v2.0 \
           --action report

If you decide to rename the containers instead of just print messages, use
`--action rename` instead.

Migrating data from RGW to Swift
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Please remember to clear your environment variables before running the script::

    unset `env | grep OS_ | awk -F "=" '{print $1}' | xargs`

In order to migrate from RGW to Swift, it is necessary to run both systems
simultaneously. However the Openstack endpoint(s) for Object Storage can only
point to one of these - referred to as the default Object Storage System.
Currently (and for the entire migration exercise) this will be RGW.

1. Prerequisites

    * You have an credential with admin role. It's recommended to create a
      dedicated user/tenant for migration.
    * It's recommended running this script on API (or Swift Proxy) node in each
      region.
    * python-swiftclient and python-keystoneclient need to be installed.

2. Before actual moving objects from RGW to Swift, you can see the overview of
   object storage statistics in RGW::

    $ python -W ignore swift-migrate.py -u openstack:objectmonitor \
             -r test-1 -a https://api.ostst.wgtn.cat-it.co.nz:5000/v2.0 、
             -c 2 | tee output

   You will see logs both in screen and output file, in the end, you can see
   the total number of containers/objects/bytes and the top 10 tenants of
   object storage usage in your system(RGW in this case).

   At the meanwhile, you can see total number of containers/objects/bytes of
   each tenant in each process's log file.

   More details could be shown if you specify -v in the command line.

3. Start to migrate object from RGW to Swift::

    python -W ignore swift-migrate.py -u openstack:objectmonitor \
             -r test-1 -a https://api.ostst.wgtn.cat-it.co.nz:5000/v2.0 \
             -t copy -x api.ostst.wgtn.cat-it.co.nz -c 2 | tee output

   * Multiple processes will be created for the migration job, the default
     concurrency number is processor number of the host the script is running
     on. Tenants will be split evenly among all the processes.
   * The migration time will depends on how many tenants in the environment and
     how many objects and how big of them in each tenant.
   * You can specify the tenant names you want to include or exclude.
   * Containers and objects will be created in Swift if not exist or changed
     since last running.
   * For object with size less than 5G that users uploaded using S3 multi-part
     upload API to RGW, a single object will be created in Swift. the Etag of
     original object will be stored in object metadata in Swift, metadata
     key: `x-object-meta-old-hash`
   * When migrating single large object (with size > 5G)from RGW to Swift, the
     object will be split into multiple segments(with size of each equals 2G by
     default) and uploaded as dynamic large object.
   * The script will check the Etag of every object after migration.

4. Now, all you need to do is wait and pray :-)

Check additional containers/objects in Swift
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Migration will last for a long duration and will be triggered multiple times
before we make Swift as the default service in Keystone in the end. Users still
can access their data in RGW for updating, deleting, etc. The update case is
already considered in `swift-migrate.py` script, but for containers/objects
that were deleted by users, `swift-check-deleted.py` script will be used to
identify them.

You can use the following command for the check::

    python swift-check-deleted.py openstack:objectmonitor \
            https://api.ostst.wgtn.cat-it.co.nz:5000/v2.0 \
            api.ostst.wgtn.cat-it.co.nz \
            --env preprod \
            --action report

If you decide to delete the containers/objects instead of just print messages,
use `--action delete` instead.

Please remember to clear your environment variables before running the script::

    unset `env | grep OS_ | awk -F "=" '{print $1}' | xargs`

NOTE: Additional container for object segments could be created when migrating
single large object from RGW to Swift, and should not be deleted in this case.
If the original single large object has been deleted by users after the last
migration running, deleting the object in Swift will delete its segments in
that container automatically. The side-effect is, there may be some empty
segment containers(with suffix `_segments`) left in tenant account. It is not
harmful though because it will still be created if users upload dynamic large
object.