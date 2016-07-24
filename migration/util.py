import itertools
import math

from keystoneclient.v2_0 import client as k_client
import swiftclient
from swiftclient.service import SwiftService


def _chunks(arr, m):
    """Split the arr into M chunks."""
    n = int(math.ceil(len(arr) / float(m)))
    return [arr[i:i + n] for i in range(0, len(arr), n)]


def _keystone_connect(user_name, tenant_name, key, insecure, auth_version,
                      auth_url, options={}):
    keycon = k_client.Client(
        username=user_name,
        tenant_name=tenant_name,
        password=key,
        insecure=insecure,
        auth_version=auth_version,
        auth_url=auth_url,
        os_options=options,
    )
    return keycon


def _get_tenants_group(tenants, args, multiprocess=False):
    """Get tenants groups needed to be handled by multi-processes.

    Example1, we have 5 tenants [1, 2, 3, 4, 5], with concurrency 2, the
    result groups will be: [[1, 2, 3], [4, 5]]

    Example2, we have 5 tenants [1, 2, 3, 4, 5], with concurrency 3 or 4, the
    result groups will be: [[1, 2], [3, 4], [5, 6]]
    """
    tenants_map = {}
    for t in tenants:
        tenants_map.update({t.name: t})

    actual_tnames = tenants_map.keys()

    if args.include_tenants:
        invalid_tenants = set(args.include_tenants) - set(tenants_map.keys())
        if invalid_tenants:
            print('Invalid tenants: %s' % invalid_tenants)
            exit(1)

        actual_tnames = args.include_tenants
    elif args.exclude_tenants:
        actual_tnames = set(tenants_map.keys()) - set(args.exclude_tenants)

    actual_tenants = [tenants_map[name] for name in actual_tnames]

    if multiprocess:
        tenants_group = _chunks(actual_tenants, args.concurrency)
    else:
        tenants_group = [actual_tenants]

    return tenants_group


def _check_tenant_membership(keycon, username, rolename, tenants):
    """Ensure user has role in every tenant."""
    for user in keycon.users.list():
        if user.name == username:
            break
    else:
        raise RuntimeError('failed to find own user!')

    for member in keycon.roles.list():
        if member.name == rolename:
            break
    else:
        raise RuntimeError('failed to find member role!')

    for tenant in tenants:
        print "...checking tenant: " + tenant.name + ", user: " + username
        is_ok = False
        for role in keycon.roles.roles_for_user(user, tenant):
            if role.name == 'admin' or role.name == rolename:
                is_ok = True
                break
        if is_ok == False:
            print "......adding " + rolename + " role for this tenant"
            keycon.roles.add_user_role(user, member, tenant)


def check_user_access(args, key, multiprocess=False):
    tenant_name = args.user.split(':')[0]
    user_name = args.user.split(':')[1]

    keycon = _keystone_connect(
        user_name, tenant_name, key, True, 2, args.authurl,
        # {'tenant_name': tenant_name, 'region_name': args.region},
    )

    tenants = [t for t in keycon.tenants.list() if t.enabled]
    tenants_group = _get_tenants_group(tenants, args, multiprocess)

    print("checking user has %s role in each tenant." % args.role)
    _check_tenant_membership(keycon, user_name, args.role,
                             itertools.chain.from_iterable(tenants_group))

    return tenants_group


def get_connection(tenant_name, user_name, key, auth_url, options={}):
    return swiftclient.Connection(
        user=tenant_name + ':' + user_name,
        key=key,
        authurl=auth_url,
        insecure=True,
        auth_version=2,
        os_options=options,
    )


def get_service_client(tenant_name, user_name, key, auth_url, options={}):
    return SwiftService(
        options=dict(
            {
                "auth_version": 2,
                "os_username": user_name,
                "os_password": key,
                "os_tenant_name": tenant_name,
                "os_auth_url": auth_url,
                "insecure": True
            },
            **options
        )
    )


def get_all_containers(srv_client):
    containers = []

    for page in srv_client.list():
        if page["success"]:
            for container in page["listing"]:
                containers.append(container)
        else:
            raise Exception(page["error"])

    return containers


def get_all_objects(srv_client, container_name):
    objects = []

    for page in srv_client.list(container=container_name):
        if page["success"]:
            for object in page["listing"]:
                objects.append(object)
        else:
            raise Exception(page["error"])

    return objects


def delete_container(client, name):
    del_iter = client.delete(container=name)
    for del_res in del_iter:
        if not del_res['success']:
            raise Exception(del_res['error'])


def delete_objects(client, cname, onames):
    del_iter = client.delete(container=cname, objects=onames)
    for del_res in del_iter:
        if not del_res['success']:
            raise Exception(del_res['error'])


def rename_container(conn, name, suffix):
    new_name = '%s-%s' % (name, suffix)

    try:
        conn.head_container(new_name)
        print('\t\tContainer: %s already exists.' % new_name)
    except ClientException:
        print('\t\tCreating new container: %s' % new_name)

        # Copy acls if it is defined.
        src_chead = conn.head_container(name)
        tgt_chead = {}
        if src_chead.has_key('x-container-read'):
            tgt_chead['x-container-read'] = src_chead['x-container-read']
        if src_chead.has_key('x-container-write'):
            tgt_chead['x-container-write'] = src_chead['x-container-write']

        conn.put_container(container=new_name, headers=tgt_chead)

    objects = conn.get_container(container=name, full_listing=True)[1]
    obj_names = [obj['name'] for obj in objects]

    print('\t\tCopying objects from %s to %s' % (name, new_name))

    for o_name in obj_names:
        old_obj_path = '/%s/%s' % (name, o_name)

        conn.put_object(new_name, o_name, None, content_length=0,
                        headers={'X-Copy-From': old_obj_path})

    print('\t\tDeleting container: %s' % name)
    delete_container(conn, name)
