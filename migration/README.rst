Helper Scripts for Object Storage Migration From Ceph Rados Gateway to Swift
============================================================================

Prerequisite
~~~~~~~~~~~~
The motivation of writing those scirpts comes from the internal requirement of
Catalyst Cloud. We deployed Rados Gateway to provide object storage service to
customers of our OpenStack based public cloud in two separated regions. As
Swift becomes more stable and mature in OpenStack community, we decided to
switch to use Swift as object storage service. At the same time, we expanded
the number of regions to three, so customers could benefit from high
availability and unified interface with eventual consistency provided by Swift
multi-region duplication.

Check duplicate container name between regions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Before we do migration from RGW to Swift, the first thing we need to check is
if there are containers with same name existing in different regions, which
will lead to customers data loss.

You can use the following command for the check::

    $ python swift-check-duplicate.py openstack:objectmonitor \
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
    * Please ensure there is enough disk space(maximum of
      concurrency*max_single_object_size) in /tmp on the host the script is
      running, because the script will download large object to that folder,
      then delete it after upload.

2. Before actual moving objects from RGW to Swift, you can see the overview of
   object storage statistics in RGW::

    $ python swift-migrate.py --user openstack:objectmonitor \
             --region <region_name> --authurl https://api.cloud.catalyst.net.nz:5000/v2.0 \
             --rgw-host <rgw-api-host> --act stat

   In the end of output, you can see summary information, such as the total
   number of containers/objects/bytes and the top 10 tenants of object storage
   usage in your system(RGW in this case).

   At the meanwhile, you can see total number of containers/objects/bytes of
   each tenant in each process's log file in current directory.

   More details could be shown if you specify -v in the command line.

3. Start to migrate object from RGW to Swift::

    $ python swift-migrate.py --user openstack:objectmonitor \
             --region test-1 --authurl https://api.cloud.catalyst.net.nz:5000/v2.0 \
             --act copy --host <swift-proxy-host> --rgw-host <rgw-api-host>

   * The migration time will depends on how many tenants in the environment and
     how many objects and how big of them in each tenant.
   * You can specify the tenant names you want to include or exclude.
   * You can specify the exact container or object that to be migrated.
   * Containers and objects will be created in Swift if not exist or changed
     since last running.
   * For object with size less than 5G that users uploaded using S3 multi-part
     upload API to RGW, a single object will be created in Swift. the Etag of
     original object will be stored in object metadata in Swift, metadata
     key: `x-object-meta-old-hash`
   * When migrating single large object (with size > 5G)from RGW to Swift, the
     object will be split into multiple segments(with size of each equals 2G by
     default) and uploaded as dynamic large object in Swift.
   * Static large object is not supported in RGW 0.9.4.x.

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
            https://api.cloud.catalyst.net.nz:5000/v2.0 \
            <swift-proxy-host> \
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
