#!/usr/bin/env python3
import sys
sys.path.insert(1, '/home/BSINFRAPRD/iatanasov/reporting_project/db_py')
from base import Session
from base import Package
from sqlalchemy import or_
from sqlalchemy import MetaData
from base import Server
from datetime import datetime
import time
from packaging import version
import salt.client

global_salt_client = salt.client.LocalClient()
session = Session()
now = datetime.now()
month_year = now.strftime("%b %Y")

def is_exception_update(hn):
    with open('/opt/susemgr-scripts/exclude4patch', 'r') as f:
       content = f.readlines()
       content = [x.strip() for x in content]
       for line in content:
           if not "#" in line:
              if hn == line.split(';')[0]:
                 return True
    return False


def stop_reboot_protectior(salt_minion):
    reboot_protector_is_on = global_salt_client.cmd(salt_minion, 'service.status', ['bashrc'])
    if reboot_protector_is_on[salt_minion]:
        try:
            return global_salt_client.cmd(salt_minion, 'service.stop', ['bashrc'])[salt_minion]
        except:
            return False
    else:
        return True

def is_cluster(salt_minion):
    try:
        check_suse = global_salt_client.cmd(salt_minion, 'cmd.run', ['/usr/sbin/crm status'])
    except:
        print('System is not reachable via salt')
    try:
        check_service_guard = global_salt_client.cmd(salt_minion, 'cmd.run', ['/usr/local/cmcluster/bin/cmviewcl -v'])
    except:
        print('System is not reachable via salt')

    if 'No such' in check_suse[salt_minion] and 'No such' in check_service_guard[salt_minion]:
        return False
    else:
        return True
    
def for_sp_mig(salt_minion):
    try:
        sp_mig = global_salt_client.cmd(salt_minion, 'grains.item', ['sp_mig'])
    except:
        print('Service pack migration grain is not set')
    if sp_mig[salt_minion]['sp_mig'] and sp_mig[salt_minion]['sp_mig'][0]:
        return True
    else:
        return False
def for_wawo(salt_minion):
    try:
        is_wawo = global_salt_client.cmd(salt_minion, 'grains.item', ['is_wawo'])
    except:
        print('WAWO grain is not set')
    if is_wawo[salt_minion] and is_wawo[salt_minion]['is_wawo']:
        return True
    else:
        return False

def wawo_target(salt_minion):
    try:
        wawo_target = global_salt_client.cmd(salt_minion, 'grains.item', ['wawo_target'])
    except:
        print('WAWO target grain is not set')
    if wawo_target[salt_minion]:
        return wawo_target[salt_minion]['wawo_target'][0]
    else:
        return('no wawo target')

def ConvertSectoDay(n):
    day = n // (24 * 3600)
    n = n % (24 * 3600)
    hour = n // 3600
    n %= 3600
    minutes = n // 60
    n %= 60
    seconds = n
    return f"{day}d:{hour}h:{minutes}m:{seconds}s"

def get_current_kernel(salt_minion):
    cur_kern = global_salt_client.cmd(salt_minion, 'pkg.version', ['kernel-default'])
    if not cur_kern[salt_minion]:
        cur_kern = global_salt_client.cmd(salt_minion, 'pkg.version', ['kernel'])
    for k, v in cur_kern.items():
        kern_lst = sorted(map(version.parse, v.split(',')))
        return str(kern_lst[-1])

def get_uptime_sec(salt_minion):
    result = global_salt_client.cmd(salt_minion, 'status.uptime', [])
    for k, v in result.items():
        return v['seconds']

def is_rebooted(salt_minion):
    uptime = get_uptime_sec(salt_minion)
    if uptime < 900:
        return True
    else:
        return False

def get_patch_status(salt_minion):
    server_obj = session.query(Server).filter(Server.name == salt_minion, Server.month_year == month_year)
    kernel = get_current_kernel(salt_minion)
    for server in server_obj:
        if not server.target_kernel_version:
            return 'Missing target'
        else:
            if kernel == server.target_kernel_version:
                return 'Patched'
            elif kernel > server.target_kernel_version:
                return 'Over Patched'
            else:
                return 'Not Patched'

def get_bulk_fdom_pkgs():
    pass
    

def get_fdom_pkgs(salt_minion):
    server_obj = session.query(Server).filter(Server.name==salt_minion, Server.month_year == month_year)
    pkg_objs = []
    for server in server_obj:
        server_id = server.id
        pkgs = session.query(Package).filter(Package.server_id == server_id)
        pkg_name_id = {pkg.name:pkg.id for pkg in pkgs}
        pkg_list = [pkg for pkg in pkg_name_id.keys()]
        return pkg_list, pkg_name_id

def insert_packet_results(salt_minion):
    pkg_objs = []
    pkgs, pkg_name_id = get_fdom_pkgs(salt_minion)
    pkgs_current_version = global_salt_client.cmd(salt_minion, 'pkg.version', [x for x in pkgs])
    if bool(pkgs_current_version):
        if isinstance(pkgs_current_version[salt_minion], dict):
            for pkg_name, curr_vers in pkgs_current_version[salt_minion].items():
                if ',' in curr_vers:
                    sorted_lst = sorted(map(version.parse, curr_vers.split(',')))
                    curr_vers = str(sorted_lst[-1])
                pkg_objs.append(dict(id=pkg_name_id[pkg_name], current_version=curr_vers))
    try:
        session.bulk_update_mappings(Package, pkg_objs)
        session.commit()
        session.close()
    except:
        print('cannot commit to postgres')

def get_system_os_version(salt_minion):
    try:
        s = global_salt_client.cmd(salt_minion, 'grains.item', ['os_family', 'osrelease'])
    except:
        print(f'Minion on {salt_minion} is not responsible!')
    for k, v in s.items():
        if isinstance(v, dict):
            os_release  = f'{v.get("os_family")} {v.get("osrelease")}'
            return os_release

def insert_patching_result(salt_minion):
    server_objs = []
    server_obj = session.query(Server).filter(Server.month_year == month_year, 
            Server.name == salt_minion)
    for item in server_obj:
        server_id = item.id
    uptime = ConvertSectoDay(get_uptime_sec(salt_minion))
    kernel_version = get_current_kernel(salt_minion)
    os_release = get_system_os_version(salt_minion)
    status = get_patch_status(salt_minion)
    server_objs.append(dict(id=server_id, os_release=os_release,patched=status,
                                    uptime=uptime,kernel_version=kernel_version))
    session.bulk_update_mappings(Server,server_objs)
    session.commit()
    session.close()


def update_current_packages(salt_minion):
    server_obj = session.query(Server).filter(Server.month_year == month_year,
                        Server.name == salt_minion)
    for item in server_obj:
        server_id = item.id
    package_objs = []
    kernel_objs = []
    pkg_objs = []
    result = global_salt_client.cmd(salt_minion, 'pkg.list_upgrades', [])
    if isinstance(result[salt_minion], dict):
        if bool(result[salt_minion]):
            for pkg_name,pkg_upg_vers in result[salt_minion].items():
                if pkg_name != 'retcode':
                    if pkg_name == 'kernel' or pkg_name == 'kernel-default':
                        kernel_objs.append(dict(id=server_id, target_kernel_version=pkg_upg_vers))
                    package_objs.append(dict(name=pkg_name, 
                                             current_version='',
                                             upgradable_version=pkg_upg_vers,
                                             server_id=server_id))
        else:
            package_objs.append(dict(name = '',
                                     current_version="",
                                     upgradable_version="",
                                     server_id=server_id))
    else:
        print('Server has issues with repos')
    session.bulk_insert_mappings(Package,package_objs)
    session.bulk_update_mappings(Server, kernel_objs)
    session.commit()
    session.close()
