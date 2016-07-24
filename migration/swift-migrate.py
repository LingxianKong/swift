#!/usr/bin/env python
#
# swift-migrate.py:
#
# Author:
# mark.kirkwood@catalyst.net.nz
#
# License:
# Apache License, Version 2.0 (same as Openstack)
#
# Desc:
# Migrates containers and objects from one object system (Swift
# or RGW based) to another (Swift based).
#
# Assumptions/requirements:
# - both systems use the same keystone database for auth
# - target system is Swift based
# - user running the migration is admin in primary tenancy

# Design overview:
# - add self to every tenancy with supplied role
# - Divide tenancies according to process number configuration, loop every tenancy in separated process.
# -- connect to src and (optionally) target object storage systems as tenant
# -- get container and object stats for src
# -- copy containers and objects src to target if in copy mode

import argparse
from collections import Iterable
import getpass
import json
import multiprocessing
import os
import re
import tempfile
import time

import six
import swiftclient
from swiftclient.service import SwiftError
from swiftclient.service import SwiftUploadObject

import util

# need b6457e0f95c2563f745bbfb64c739929bc0dc901 for body
# response object in get_object that supports read() method
if swiftclient.__version__ < '2.6.0':
    raise RuntimeError('newer swiftclient required')

# The custom header we specify when creating object in Swift, to help us
# indentify original large object uploaded with S3 API in RGW.
OLD_HASH_HEADER = 'x-object-meta-old-hash'

# A simple regex that matches large object hash which uploaded with S3
# multi-part upload API.
HASH_PATTERN = re.compile('\w+-\w+')

GB_5 = 5368709120
GB_SPLIT = 2147483648


def _print_object_detail(src_srvclient, tenant_name, cname, content,
                         max_size_info):
    for page in src_srvclient.list(container=cname):
        if page["success"]:
            for item in page["listing"]:
                if item['bytes'] > max_size_info['size']:
                    max_size_info.update({
                        'tenant': tenant_name,
                        'size': item['bytes'],
                        'container': cname,
                        'object': item['name']
                    })

                prefix = ('[large-object] '
                          if HASH_PATTERN.match(item['hash'])
                          else '')

                obj_stat = list(
                    src_srvclient.stat(container=cname, objects=[item['name']])
                )[0]
                obj_header = obj_stat['headers']

                content.append(
                    '            %s%s\t%s' % (
                        prefix,
                        item['name'],
                        item['bytes'],
                    )
                )
                content.append('            ....headers: %s' % obj_header)
        else:
            raise Exception(page["error"])


def stat_tenant(id, content, src_srvclient, verbose, max_size_info,
                tenant_name):
    for page in src_srvclient.list():
        if page["success"]:
            for container in page["listing"]:
                cname = container['name']
                print('...[%02d] Processing container %s' % (id, cname))

                content.append(
                    '........{0}, objects: {1}\tbytes: {2}'.format(
                        cname, container['count'], container['bytes'])
                )

                # Print objects details.
                if verbose:
                    _print_object_detail(src_srvclient, tenant_name, cname,
                                         content, max_size_info)
        else:
            raise Exception(page["error"])


def check_migrate_object(container_name, src_object, src_header,
                         tgt_srvclient):
    """Check whether we should migrate src object or not.

    Return True if migration is needed, otherwise return False.
    """
    tgt_obj = list(tgt_srvclient.stat(container=container_name,
                                      objects=[src_object['name']]))[0]

    if not tgt_obj['success']:
        return True

    tgt_header = tgt_obj['headers']

    # For multi-part large object hash check.
    if (HASH_PATTERN.match(src_object['hash']) and
                tgt_header.get(OLD_HASH_HEADER, '') == src_object['hash']):
        return False

    # For DLO, ignore the check if content-length is equal. Because the etag
    # will be different.
    if src_header.get('x-object-manifest', False):
        origin_length = int(src_header['content-length'])
        tgt_length = int(tgt_header['content-length'])

        if origin_length == tgt_length:
            return False

    # For normal object etag check.
    if tgt_header['etag'] == src_object['hash']:
        return False

    return True


def check_migrate_after(container_name, src_object, tgt_srvclient, is_dlo,
                        content):
    content.append("             ..ok..checking")

    tgt_obj = list(
        tgt_srvclient.stat(
            container=container_name,
            objects=[src_object['name']])
    )[0]

    if not tgt_obj['success']:
        raise Exception(tgt_obj['error'])

    if not is_dlo:
        tgt_header = tgt_obj['headers']
        if (not tgt_header.get(OLD_HASH_HEADER, False) and
                    tgt_header['etag'] != src_object['hash']):
            raise Exception('src and target objects have different hashes.')

    content.append("             ..ok")


def migrate_DLO(container_name, src_object, src_head, src_srvclient,
                tgt_srvclient):
    """Migrate dynamic large object."""
    headers = ['x-object-manifest:%s' % src_head['x-object-manifest']]

    upload_iter = tgt_srvclient.upload(
        container_name,
        [SwiftUploadObject(None, object_name=src_object['name'])],
        options={'header': headers}
    )

    for r in upload_iter:
        if not r['success']:
            raise Exception(r['error'])


def migrate_SLO(container_name, src_object, src_head, src_srvclient,
                tgt_srvclient):
    """Migrate static large object.

    Note that static large object (slo) does not actually work with RGW as of
    0.9.4.x, so hopefully not too many of these. We migrate them anyway.

    It's a little difficult to upload the SLO manifest file before we are sure
    all the segments are uploaded to Swift. When the PUT operation sees the
    ?multipart-manifest=put query parameter, it reads the request body and
    verifies that each segment object exists and that the sizes and ETags
    match. If there is a mismatch, the PUT operation fails.
    """
    with tempfile.NamedTemporaryFile() as temp_file:
        down_res_iter = src_srvclient.download(
            container=container_name,
            objects=[src_object['name']],
            options={'out_file': temp_file.name, 'checksum': False}
        )

        for down_res in down_res_iter:
            if down_res['success']:
                headers = ['x-static-large-object:True']

                upload_iter = tgt_srvclient.upload(
                    container_name,
                    [SwiftUploadObject(temp_file.name,
                                       object_name=src_object['name'])],
                    options={'header': headers}
                )
                for r in upload_iter:
                    if not r['success']:
                        raise Exception(r['error'])
            else:
                raise Exception(down_res['error'])

    # src_objtype = src_head['content-type']
    # src_objlen = src_head['content-length']
    #
    # tgt_ohead = {}
    # tgt_ohead['x-static-large-object'] = src_head['x-static-large-object']
    #
    # src_obj = src_swiftcon.get_object(
    #     container_name, src_object['name'],
    #     query_string='multipart-manifest=get'
    # )
    # src_objdata = src_obj[1]
    #
    # # reconstruct valid manifest from saved debug manifest object
    # # body - the dict keys are *wrong*...wtf
    # tgt_parsed_objdata = []
    # src_parsed_objdata = json.loads(src_objdata)
    #
    # for parsed_item in src_parsed_objdata:
    #     parsed_item['size_bytes'] = parsed_item.pop('bytes')
    #     parsed_item['etag'] = parsed_item.pop('hash')
    #     parsed_item['path'] = parsed_item.pop('name')
    #     for key in parsed_item.keys():
    #         if (key != 'size_bytes' and key != 'etag'
    #             and key != 'path' and key != 'range'):
    #             parsed_item.pop(key)
    #     tgt_parsed_objdata.append(parsed_item)
    #
    # tgt_objdata = json.dumps(tgt_parsed_objdata)
    #
    # tgt_swiftcon.put_object(container_name, src_object['name'],
    #                         contents=tgt_objdata, content_type=src_objtype,
    #                         content_length=src_objlen, headers=tgt_ohead)


class _ReadableContent(object):
    def __init__(self, reader):
        self.reader = reader
        self.content_iterator = iter(self.reader)

    def read(self, chunk_size):
        """Read content of object.

        the 'chunk_size' param is not used here, but it's passed by uploading
        process. For object downloading and uploading, the default
        chunk size is the same(65536 by default).
        """
        return self.content_iterator.next()


def get_object_user_meta(object_header):
    user_meta_list = []

    for (m_key, m_value) in six.iteritems(object_header):
        if m_key.lower().startswith('x-object-meta-'):
            user_meta_list.append('%s:%s' % (m_key, m_value))

    return user_meta_list


def migrate_object(container_name, src_object, src_head, src_srvclient,
                   tgt_srvclient, content):
    """Migrate normal object."""
    single_large_object = True if int(src_object['bytes']) > GB_5 else False

    # Get user's customized object metadata, format:
    # X-<type>-Meta-<key>: <value>
    # http://docs.openstack.org/developer/swift/development_middleware.html#swift-metadata
    user_meta = get_object_user_meta(src_head)

    header_list = []
    if HASH_PATTERN.match(src_object['hash']):
        header_list.append('%s:%s' % (OLD_HASH_HEADER, src_object['hash']))
    header_list.extend(user_meta)

    if single_large_object:
        content.append('            ..[large object] '
                       'download...split...upload')

        with tempfile.NamedTemporaryFile() as temp_file:
            down_res = list(src_srvclient.download(
                container=container_name,
                objects=[src_object['name']],
                options={'out_file': '-', 'checksum': False}
            ))[0]
            contents = down_res['contents']
            assert isinstance(contents, Iterable)

            # Hacking here to avoid download checksum.
            # TODO: Remove this when swiftclient version > 3.0.0
            contents._expected_etag = None

            for chunk in contents:
                temp_file.file.write(chunk)

            # Upload large object with segments.
            upload_iter = tgt_srvclient.upload(
                container_name,
                [SwiftUploadObject(temp_file.name,
                                   object_name=src_object['name'])],
                options={'header': header_list,
                         'segment_size': GB_SPLIT,
                         'checksum': False}
            )
            for r in upload_iter:
                if not r['success']:
                    raise Exception(r['error'])
    else:
        # Download normal object, the contents in response is an iterable
        # object. We don't do checksum for download/upload, will do that
        # outside this method.
        down_res = list(
            src_srvclient.download(
                container=container_name,
                objects=[src_object['name']],
                options={'out_file': '-', 'checksum': False}
            )
        )[0]
        contents = down_res['contents']
        assert isinstance(contents, Iterable)

        # Hacking here to avoid download checksum.
        # TODO: Remove this when swiftclient version > 3.0.0
        contents._expected_etag = None

        readalbe_content = _ReadableContent(contents)

        upload_iter = tgt_srvclient.upload(
            container_name,
            [SwiftUploadObject(readalbe_content,
                               object_name=src_object['name'])],
            options={'header': header_list, 'checksum': False}
        )

        for r in upload_iter:
            if not r['success']:
                raise Exception(r['error'])


def migrate_container(container_name, src_srvclient, tgt_srvclient, content):
    for page in src_srvclient.list(container=container_name):
        if not page["success"]:
            raise Exception(page["error"])

        for src_data in page["listing"]:
            src_obj = list(
                src_srvclient.stat(
                    container=container_name,
                    objects=[src_data['name']])
            )[0]
            src_ohead = src_obj['headers']

            is_dlo = src_ohead.get('x-object-manifest', False)

            # First, check if migration is needed.
            if not check_migrate_object(container_name, src_data, src_ohead,
                                        tgt_srvclient):
                content.append(
                    '            existing object: %s' % src_data['name'])
                continue

            content.append(
                '            creating object: %s,\tbytes: %s' %
                (src_data['name'], src_data['bytes']))

            try:
                if is_dlo:
                    migrate_DLO(container_name, src_data, src_ohead,
                                src_srvclient, tgt_srvclient)
                elif src_ohead.get('x-static-large-object', False):
                    migrate_SLO(container_name, src_data, src_ohead,
                                src_srvclient, tgt_srvclient)
                else:
                    migrate_object(container_name, src_data, src_ohead,
                                   src_srvclient, tgt_srvclient, content)

                # Check hash and etag after uploading, don't check DLO.
                check_migrate_after(container_name, src_data, tgt_srvclient,
                                    is_dlo, content)
            except Exception as e:
                content.append("             ..failed. Reason: %s" % str(e))


def migrate_tenant(id, content, src_srvclient, tgt_srvclient):
    for page in src_srvclient.list():
        if page["success"]:
            for container in page["listing"]:
                cname = container['name']
                print('...[%02d] Processing container %s' % (id, cname))

                containe_stat = src_srvclient.stat(container=cname)
                header = containe_stat['headers']

                header_list = []
                if header.has_key('x-container-read'):
                    header_list.append(
                        "X-Container-Read:%s" % header['x-container-read'])
                if header.has_key('x-container-write'):
                    header_list.append(
                        "X-Container-Write:%s" % header['x-container-write'])
                tgt_options = {'header': header_list}

                # create container in target if it does not exist
                try:
                    tgt_srvclient.stat(container=cname)
                except SwiftError:
                    content.append('........creating container: %s' % cname)
                    try:
                        tgt_srvclient.post(container=cname,
                                           options=tgt_options)
                        content.append("........ok")
                    except SwiftError as e:
                        content.append("........failed. Reason: %s" % str(e))
                        continue
                else:
                    content.append('........existing container: %s' % cname)

                migrate_container(cname, src_srvclient, tgt_srvclient, content)

        else:
            raise Exception(page["error"])


def _get_connections(tenant, args, key):
    tgt_swiftcon = None

    if args.default_storage == 'rgw':
        # Get RGW connection from Keystone.
        src_swiftcon = util.get_connection(
            tenant.name,
            args.user.split(':')[1],
            key,
            args.authurl,
            {'tenant_name': tenant.name, 'region_name': args.region}
        )

        if args.act == 'copy':
            storurl = 'https://%s:%s/v1/AUTH_%s' % (
                args.host, args.port, tenant.id)

            tgt_swiftcon = util.get_connection(
                tenant.name,
                args.user.split(':')[1],
                key,
                args.authurl,
                {'tenant_name': tenant.name, 'region_name': args.region,
                 'object_storage_url': storurl}
            )
    else:
        storurl = 'https://%s:%s/swift/v1' % (args.host, args.port)
        src_swiftcon = util.get_connection(
            tenant.name,
            args.user.split(':')[1],
            key,
            args.authurl,
            {'tenant_name': tenant.name, 'region_name': args.region,
             'object_storage_url': storurl}
        )

        if args.act == 'copy':
            # Get Swift connection from Keystone.
            tgt_swiftcon = util.get_connection(
                tenant.name,
                args.user.split(':')[1],
                key,
                args.authurl,
                {'tenant_name': tenant.name, 'region_name': args.region}
            )

    return src_swiftcon, tgt_swiftcon


def _get_service_clients(tenant, args, key):
    tgt_srvclient = None

    if args.default_storage == 'rgw':
        # Get RGW service client from Keystone.
        src_srvclient = util.get_service_client(
            tenant.name,
            args.user.split(':')[1],
            key,
            args.authurl,
            {'os_region_name': args.region}
        )

        if args.act == 'copy':
            storurl = 'https://%s:%s/v1/AUTH_%s' % (
                args.host, args.port, tenant.id)

            tgt_srvclient = util.get_service_client(
                tenant.name,
                args.user.split(':')[1],
                key,
                args.authurl,
                {'os_region_name': args.region, 'os_storage_url': storurl}
            )
    else:
        storurl = 'https://%s:%s/swift/v1' % (args.host, args.port)
        src_srvclient = util.get_service_client(
            tenant.name,
            args.user.split(':')[1],
            key,
            args.authurl,
            {'os_region_name': args.region, 'os_storage_url': storurl}
        )

        if args.act == 'copy':
            # Get Swift service clent from Keystone.
            tgt_srvclient = util.get_service_client(
                tenant.name,
                args.user.split(':')[1],
                key,
                args.authurl,
                {'os_region_name': args.region}
            )

    return src_srvclient, tgt_srvclient


def worker(id, tenants, lock, stats, tenant_usage, args, key):
    file_name = ("swift-migrate-worker-%02d.output" % id)
    max_size_info = {'tenant': '', 'container': '', 'object': '', 'size': 0}
    tenant_usage_map = {}

    # Remove the log file first.
    if os.path.exists(file_name):
        os.remove(file_name)

    for tenant in tenants:
        content = []

        try:
            print('[%02d] processing tenant: %s' % (id, tenant.name))
            content.append("....processing tenant " + tenant.name)

            src_srvclient, tgt_srvclient = _get_service_clients(
                tenant, args, key)

            with src_srvclient:
                accout_stat = src_srvclient.stat()
                account = accout_stat['headers']

                content.append(
                    "......containers: {0},\tobjects: {1},\tbytes: {2}".format(
                        account['x-account-container-count'],
                        account['x-account-object-count'],
                        account['x-account-bytes-used']
                    )
                )
                tenant_usage[tenant.name] = int(
                    account['x-account-bytes-used']
                )

                with lock:
                    stats['cons'] += int(account['x-account-container-count'])
                    stats['objs'] += int(account['x-account-object-count'])
                    stats['bytes'] += int(account['x-account-bytes-used'])

                if args.act == 'stat':
                    stat_tenant(id, content, src_srvclient, args.verbose,
                                max_size_info, tenant.name)

                if args.act == 'copy':
                    with tgt_srvclient:
                        migrate_tenant(id, content, src_srvclient,
                                       tgt_srvclient)
        except Exception as e:
            print(
                '[%02d] error occured when processing tenant: %s. error: %s' %
                (id, tenant.name, str(e))
            )
        finally:
            with open(file_name, 'a') as file:
                file.write('\n'.join(content))
                file.write('\n')

    # Print max object information.
    if args.act == 'stat' and args.verbose:
        with open(file_name, 'a') as file:
            file.write('\nmax object size info: %s' % max_size_info)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u", "--user",
        help="Combination of admin tenant name and user name. Example: "
             "openstack:objectmonitor"
    )
    parser.add_argument(
        "-r", "--region",
        help="Region in which the migration needs to happen"
    )
    parser.add_argument("-x", "--host", help="Swift proxy host name")
    parser.add_argument(
        "-p", "--port",
        help="Swift proxy port, Default: 8843",
        default="8843"
    )
    parser.add_argument("-a", "--authurl", help="Keystone auth url")
    parser.add_argument(
        "-m", "--role",
        help="Name of the role that is added to each tenant for resource "
             "access. Default: admin",
        default="admin"
    )
    parser.add_argument(
        "-t", "--act",
        choices=['stat', 'copy'],
        default="stat",
        help="Action to be performed. 'stat' means only get statistic of "
             "object storage without migration, 'copy' means doing migration. "
             "Default: stat"
    )
    parser.add_argument(
        "-v", "--verbose",
        action='store_true',
        help="verbose",
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        help="Number of processes need to be running. Default: number of "
             "CPUs in the host that the script is running on.",
        default=multiprocessing.cpu_count()
    )
    parser.add_argument(
        "-s",
        "--default-storage",
        choices=['rgw', 'swift'],
        default="rgw",
        help="Default object storage service in Keytone. Default: rgw."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '-i', '--include-tenants', nargs='*',
        help="List of tenants to include in migration, delimited "
             "by whitespace."
    )
    group.add_argument(
        '-e', '--exclude-tenants', nargs='*',
        help="List of tenants to exclude in migration, delimited "
             "by whitespace."
    )
    args = parser.parse_args()

    key = getpass.getpass('enter password for ' + args.user + ': ')

    tenants_group = util.check_user_access(args, key, multiprocess=True)

    print("\nStart migration in %s processes. The output of each process is "
          "contained in separated file under the script's directory.\n"
          % len(tenants_group))

    jobs = []
    lock = multiprocessing.Lock()
    manager = multiprocessing.Manager()
    stats = manager.dict({'cons': 0, 'objs': 0, 'bytes': 0})
    tenant_usage = manager.dict()

    elapsed = time.time()
    for i in range(len(tenants_group)):
        p = multiprocessing.Process(
            target=worker,
            args=(i, tenants_group[i], lock, stats, tenant_usage, args, key)
        )
        jobs.append(p)
        p.start()
    for p in jobs:
        p.join()
    elapsed = time.time() - elapsed

    print 70 * '='
    print "elapsed: {0} s".format(elapsed)
    print "total containers: {0}\tobjects: {1}\tbytes: {2}".format(
        stats['cons'], stats['objs'], stats['bytes'])
    print 70 * '='
    # Print top 10 tenant usage in descending order.
    sorted_tenant_usage = sorted(
        tenant_usage.items(), key=lambda d: d[1], reverse=True)
    print('TOP 10 Tenants:')
    msg = []
    for name, usage in sorted_tenant_usage[:10]:
        print('%s: %s' % (name, usage))

if __name__ == '__main__':
    main()
