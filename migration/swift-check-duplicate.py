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

import util

REGION_SUFFIX_MAP = {'nz-por-1': 'por', 'nz_wlg_2': 'wlg'}


def _get_connections(tenant_name, user_name, key, auth_url):
    conns = {}

    for (region, suffix) in six.iteritems(REGION_SUFFIX_MAP):
        conn = util.get_connection(
            tenant_name,
            user_name,
            key,
            auth_url,
            {'tenant_name': tenant_name, 'region_name': region}
        )

        conns[region] = conn

    return conns


def _close_connections(conns):
    for conn in conns:
        conn.close()


def _check_duplicate(tenants, args, key, keyconn, user, role):
    user_name = args.user.split(':')[1]

    for tenant in tenants:
        print('Checking tenant: %s' % tenant.name)

        util.check_tenant_access(args, keyconn, user, tenant, role)

        region_conns = _get_connections(tenant.name, user_name, key,
                                        args.authurl)
        assert len(region_conns) == 2

        conns = region_conns.values()

        src_names = set(
            [c['name'] for c in conns[0].get_account(full_listing=True)[1]]
        )
        dest_names = set(
            [c['name'] for c in conns[1].get_account(full_listing=True)[1]]
        )

        dup_names = src_names & dest_names

        if dup_names:
            print('..Tenant: %s has duplicate container name(s) in both '
                  'regions:' % tenant.name)

            for name in dup_names:
                print('....Container name: %s' % name)

                if args.action == 'rename':
                    for (region, conn) in six.iteritems(region_conns):
                        print('......Region: %s' % region)
                        util.rename_container(conn, name,
                                              REGION_SUFFIX_MAP[region])

        _close_connections(conns)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "user",
        help="Combination of migration tenant name and user name. "
             "Example: openstack:objectmonitor"
    )
    parser.add_argument(
        "authurl",
        help="Keystone auth url"
    )
    parser.add_argument(
        "--role",
        default="admin",
        help="Name of the role that is added to each tenant for resource "
             "access. Default: admin",
    )
    parser.add_argument(
        "--action",
        choices=['report', 'rename'],
        default="report",
        help="Report the duplicate resources or rename them directly."
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
    group.add_argument(
        '--include-file',
        help="A file that contains a list of project names that should be "
             "included."
    )
    group.add_argument(
        '--exclude-file',
        help="A file that contains a list of project names that should be "
             "excluded."
    )
    args = parser.parse_args()

    key = getpass.getpass('enter password for ' + args.user + ': ')
    tenant_name = args.user.split(':')[0]
    user_name = args.user.split(':')[1]

    keyconn = util.keystone_connect(
        user_name, tenant_name, key, True, 2, args.authurl,
    )
    user, role = util.get_user_role(args, keyconn, user_name, args.role)
    tenants_group = util.get_tenant_group(args, keyconn, multiprocess=False)

    print('\nStart to check duplicate container...')

    _check_duplicate(tenants_group[0], args, key, keyconn, user, role)


if __name__ == '__main__':
    main()
