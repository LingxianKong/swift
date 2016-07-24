# Copyright 2016 Catalyst IT Ltd
# Author: lingxian.kong@catalyst.net.nz
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import argparse
import getpass

import six
from swiftclient.service import SwiftError

import util

ENV_REGIONS = {'preprod': ['test-1'], 'prod': ['nz-por-1', 'nz_wlg_2']}


def check_objects(swift_client, actual_client, cname, objects,
                  action='report'):
    objs_delete = []

    res_iter = actual_client.stat(container=cname, objects=objects)

    for res in res_iter:
        if not res['success']:
            print('.........FOUND nonexistent object: %s' % res['object'])
            objs_delete.append(res['object'])

    if action == 'delete' and objs_delete:
        util.delete_objects(swift_client, cname, objs_delete)
        print('...........deleted.')


def check_container(swift_client, rgw_clients, action):
    container_map = {}

    for container in util.get_all_containers(swift_client):
        cname = container['name']

        # Check if the container is for segments.
        if cname.endswith('_segments'):
            continue

        # For container in Swift, it may exist in either region in RGW.
        for (region, client) in six.iteritems(rgw_clients):
            try:
                res = client.stat(container=cname)
            except SwiftError:
                pass
            else:
                # Assume we don't have duplicate container name between regions
                container_map[cname] = {'region': region, 'client': client}
                break

        if not container_map.get(cname, None):
            print('......FOUND nonexistent container: %s' % cname)
            if action == 'delete':
                util.delete_container(swift_client, cname)
                print('........deleted.')
            continue

        print('......Checking container: %s in region %s' %
              (cname, container_map[cname]['region']))

        objects = util.get_all_objects(swift_client, cname)
        if not objects:
            continue

        obj_names = [obj['name'] for obj in objects]
        actual_client = container_map[cname]['client']

        check_objects(swift_client, actual_client, cname, obj_names,
                      action)


def check_deleted(tenants, args, key):
    user_name = args.user.split(':')[1]

    for tenant in tenants:
        print('Checking tenant: %s' % tenant.name)

        # Request to Swift from different regions has the same result.
        storurl = ('https://%s:%s/v1/AUTH_%s' %
                   (args.host, args.port, tenant.id))
        swift_client = util.get_service_client(
            tenant.name, user_name, key, args.authurl,
            options={'os_region_name': ENV_REGIONS[args.env][0],
                     'os_storage_url': storurl}
        )

        # Get RGW connections of all regions.
        rgw_clients = {}
        for region in ENV_REGIONS[args.env]:
            rgw_client = util.get_service_client(
                tenant.name, user_name, key, args.authurl,
                options={'os_region_name': region}
            )
            rgw_clients[region] = rgw_client

        try:
            with swift_client:
                check_container(swift_client, rgw_clients, action=args.action)
        except Exception as e:
            print('...Error: %s' % str(e))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "user",
        help="Combination of migration tenant name and user name. "
             "Example: openstack:objectmonitor"
    )
    parser.add_argument("authurl", help="Keystone auth url")
    parser.add_argument("host", help="Swift service host name")
    parser.add_argument(
        "--env",
        choices=['preprod', 'prod'],
        default="preprod",
        help="In which environment the checking is running",
    )
    parser.add_argument(
        "--port",
        default="8843",
        help="Swift service port, Default: 8843",
    )
    parser.add_argument(
        "--role",
        default="admin",
        help="Name of the role that is added to each tenant for resource "
             "access. Default: admin",
    )
    parser.add_argument(
        "--action",
        choices=['report', 'delete'],
        default="report",
        help="Report the non-exist resources or delete them directly."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '-i', '--include-tenants',
        nargs='*',
        help="List of tenants to include in checking, delimited by whitespace."
    )
    group.add_argument(
        '-e', '--exclude-tenants',
        nargs='*',
        help="List of tenants to exclude in checking, delimited by whitespace."
    )
    args = parser.parse_args()

    key = getpass.getpass('enter password for ' + args.user + ': ')

    tenants = util.check_user_access(args, key, multiprocess=False)

    print('=' * 60)

    check_deleted(tenants[0], args, key)


if __name__ == '__main__':
    main()
