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

"""
Check if tenant has same name container in all regions.

This script only can be run in production.

Usage:
python -W ignore swift-check-duplicate.py openstack:objectmonitor \
        https://api.cloud.catalyst.net.nz:5000/v2.0 \
        --action report

if you decide to rename the containers instead of just print messages, use
`--action rename` instead.
"""

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

        conns[suffix] = conn

    return conns


def _close_connections(conns):
    for conn in conns:
        conn.close()


def _check_duplicate(tenants, args, key):
    user_name = args.user.split(':')[1]

    for tenant in tenants:
        print('Checking tenant: %s' % tenant.name)

        suffix_conns = _get_connections(tenant.name, user_name, key,
                                        args.authurl)
        assert len(suffix_conns) == 2

        conns = suffix_conns.values()

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
                print('\t%s' % name)

                if args.action == 'rename':
                    for (suffix, conn) in six.iteritems(suffix_conns):
                        util.rename_container(conn, name, suffix)

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
    args = parser.parse_args()

    key = getpass.getpass('enter password for ' + args.user + ': ')

    tenants = util.check_user_access(args, key, multiprocess=False)

    print('\nStart to check duplicate container...')

    _check_duplicate(tenants[0], args, key)


if __name__ == '__main__':
    main()
