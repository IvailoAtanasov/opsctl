from base import Session
from sqlalchemy import MetaData
from base import Server
from base import Package
from sqlalchemy import or_, and_
from sqlalchemy.sql import table, column, select, update, insert
from datetime import datetime
import datetime
from general import *
import salt.client
from patching import *
from multiprocessing import Pool
now = datetime.now()
month_year = now.strftime("%b %Y")
session = Session()
local = salt.client.LocalClient()
import concurrent.futures
import csv
import subprocess

def x(server_obj):
    status = ssh_status(server_obj.name)
    server_dict = dict(id=server_obj.id, ssh_status=status)
    return server_dict

def ssh_status_list():
    '''
    this function return object with status of ssh connection between suse-mgr and target servers
    this object is used  to be pushed in lxinfra02 database
    '''
    res_objs = []
    server_q_set = session.query(Server).filter(Server.month_year == month_year)
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        server_objs = []
        for server in server_q_set:
            server_objs.append(executor.submit(x, server_obj=server))
        for i in concurrent.futures.as_completed(server_objs):
            res_objs.append(i.result())
    return res_objs

def rsyslog_server_data():
    '''
    this function return object with status of syslog port and two possible servers
    this object is used to be pushed in lxinfra02 database
    '''
    server_q_set = session.query(Server).filter(Server.month_year == month_year, Server.minion_status.contains('Up'))
    server_list = [server.name for server in server_q_set]
    server_list_id = {server.name : server.id for server in server_q_set}
    syslog_objs = []
    conf_result_dict = {}
    port_info = local.cmd(server_list, "cmd.run",['lsof -i -P -n | grep ":514 "'] , tgt_type = 'list')
    syslog_info = local.cmd(server_list, "cmd.run",[ 'grep -E -r -i "(security.bcs.de|syslog.global.basf.net)" /etc/*syslog*'],tgt_type = 'list')
    ds = [syslog_info, port_info]
    d = {}

    for k in port_info.keys():
        d[k] = tuple(d[k] for d in ds)
    
    for k,v in d.items():
        server1 = ''
        server2 = ''
        port = False
        if 'security.bcs.de' in v[0]:
            server1 = 'security.bcs.de'
        if 'syslog.global.basf.net' in v[0]:
            server2 = 'syslog.global.basf.net'
        if v[1]:
            port = True
        syslog_objs.append(dict(syslog_srv_1 = server1, syslog_srv_2=server2, port_514=port, server_id=server_list_id[k]))
    return syslog_objs
    
def salt_collected_data():
    '''
    this function return object with additional data to be populated in lxinfra02 db
    '''
    server_q_set = session.query(Server).filter(Server.month_year == month_year, Server.os_type == 'Linux')
    server_list = []
    server_list_id = {}
    minion_status = ""
    server_objs = []
    for i in server_q_set:
        server_list.append(i.name)
        server_list_id[i.name] = i.id
    s = local.cmd(server_list, ['grains.item','status.uptime', 'pkg.list_repos'], [['os_family', 'osrelease', 'kernelrelease'], [], []], tgt_type = 'list')
    for k,v in s.items():
        if isinstance(v, dict):
            os_release = f"{v['grains.item']['os_family']} {v['grains.item']['osrelease']}"
            kernel_vers = v['grains.item']['kernelrelease']
            uptime = ConvertSectoDay(int(v['status.uptime']['seconds']))
            minion_status = "Up"
            try:
                is_sap_result = v['pkg.list_repos'].keys()
            except:
                print(f'{v} has issues with repos')

        else:
            os_release = ""
            kernel_vers = ""
            uptime = ""
            app= ""
            minion_status = "Down"
        for i in is_sap_result:
            if 'sap' in i:
                is_sap = True
            else:
                is_sap = False
        server_objs.append(dict(id=server_list_id[k], name=f"{k}", os_release=f"{os_release}",
                                        uptime=f"{uptime}",kernel_version=f"{kernel_vers}", minion_status=f"{minion_status}", is_sap=is_sap))
    return server_objs

def unix_data_helper(server_obj):
    '''
    Helper function for collect_unix_data()
    '''
    
    if server_obj.os_type == 'AIX':
        out = ssh_cmd(server_obj.name, 'oslevel -s')[0]
    elif server_obj.os_type == 'HP UX':
        out = ssh_cmd(server_obj.name, "uname -a |awk '{print $3}'")[0]
    elif server_obj.os_type == 'Sun':
        out_res = ssh_cmd(server_obj.name, 'cat /etc/os-release |grep -i version_id')[0]
        out = out_res.split('=')[-1]
    else:
        out = ''
    server_dict = dict(id=server_obj.id, os_release=out)
    return server_dict

def collect_unix_data():
    '''
    this function will collect os versions for unix machines and populate them in lxinfra02 db
    '''
    server_q_set = session.query(Server).filter(Server.month_year == month_year, Server.os_type != 'Linux')
    server_objs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        res_objs = []
        for server in server_q_set:
            res_objs.append(executor.submit(unix_data_helper, server_obj=server))
        for i in concurrent.futures.as_completed(res_objs):
            server_objs.append(i.result())
    return server_objs

def collect_aix_data():
    '''
    This function creates a csv file with customer requested aix and vios data
    '''
    with open('/opt/susemgr-scripts/reports/aix_vio_weekly_data.csv', mode='w') as aix_vio:
        aix_vio_writer = csv.writer(aix_vio, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        aix_vio_writer.writerow(['HMC','SERVER','PHY_CPU','PHY_MEM','LPAR_NAME','LPAR_TYPE','LPAR_CPU','LPAR_MEM'])
        hmc_lst = ['hmc0', 'hmc04.basfad.basf.net', 'hmc9', 'hmc12', 'hmc22', 'hmc23']
        for hmc in hmc_lst:
             out =  subprocess.Popen(f"ssh hscroot@{hmc} lssyscfg -r sys -F name", shell=True, stdout=subprocess.PIPE, universal_newlines=True).communicate()[0]
             for server in out.split():
                 lpars = subprocess.Popen(f"ssh -oStrictHostKeyChecking=no hscroot@{hmc} lssyscfg -r lpar -m {server} -F 'name,lpar_env'", 
                         shell=True, stdout=subprocess.PIPE, universal_newlines=True).communicate()[0]

                 phy_cpu = subprocess.Popen(f"ssh -oStrictHostKeyChecking=no hscroot@{hmc} lshwres -r proc --level sys -m {server} -F configurable_sys_proc_units", 
                         shell=True, stdout=subprocess.PIPE, universal_newlines=True).communicate()[0]

                 phy_mem = subprocess.Popen(f"ssh -oStrictHostKeyChecking=no hscroot@{hmc} lshwres -r mem --level sys -m {server} -F configurable_sys_mem", 
                         shell=True, stdout=subprocess.PIPE, universal_newlines=True).communicate()[0]

                 for lpar in lpars.split():
                     try:
                         lpar_name, lpar_type = lpar.split(',')
                         lpar_data = subprocess.Popen(f'ssh -oStrictHostKeyChecking=no hscroot@{hmc} lshwres -r proc --level lpar -m {server} --filter "lpar_names={lpar_name}" -F "curr_proc_mode,curr_procs"', shell=True, stdout=subprocess.PIPE, universal_newlines=True).communicate()[0]

                         proc_mode, lpar_procs = lpar_data.split(',')
                         lpar_mem = subprocess.Popen(f'ssh -oStrictHostKeyChecking=no hscroot@{hmc} lshwres -r mem --level lpar -m {server} --filter "lpar_names={lpar_name}" -F "curr_mem"',
                                                         shell=True, stdout=subprocess.PIPE, universal_newlines=True).communicate()[0]
                         aix_vio_writer.writerow([hmc,server,phy_cpu.strip(),f'{int(phy_mem.strip())/1024}GB',
                             proc_mode.strip(),lpar_name.strip(),lpar_type.strip(),lpar_procs.strip(),f'{int(lpar_mem.strip())/1024}GB'])
                     except:
                         pass
def sendEmail(file_path, file_name, recipients):
    subprocess.Popen(f'mail -a {file_path} -s "{file_name}" -r "Ivaylo" {recipients} < /dev/null', shell=True, stdout=subprocess.PIPE, universal_newlines=True).communicate()[0]

def collect_fdom():
    '''
    collect all upgradable packages per system
    '''
    pkg_dict = {}
    monthly_ops_servers = session.query(Server).filter(and_(Server.minion_status.contains("Up"),Server.month_year == month_year,Server.ops_window != 'OPS_Individual')).all()

    server_list = [server.name for server in monthly_ops_servers]
    server_list_id = {server.name : server.id for server in monthly_ops_servers}
    upg_pkg = local.cmd_batch(server_list, "pkg.list_upgrades", tgt_type = 'list',batch='25%')

    kernel_objs = []
    package_objs = []

    dicts={server:pkg_dict for dicts in upg_pkg for (server,pkg_dict) in dicts.items()}

    for server, pkg_dict in dicts.items():
        if isinstance(pkg_dict, dict):
            if bool(pkg_dict):
                for pkg_name, pkg_upg_vers in pkg_dict.items():
                    if pkg_name != 'retcode':
                        if pkg_name == "kernel" or pkg_name =="kernel-default":
                            kernel_objs.append(dict(id=server_list_id[server], target_kernel_version=pkg_upg_vers))
                        package_objs.append(dict(name=pkg_name,current_version="",upgradable_version=pkg_upg_vers,server_id=server_list_id[server]))
            else:
                package_objs.append(dict(name="",current_version="",upgradable_version="",server_id=server_list_id[server]))

        else:
            print(f"{server} has issues with repos")
    return package_objs, kernel_objs

def collect_ldom():
    '''
    collect all upgradet packages to compare with fdom data
    '''
    fdom_packages = session.query(Server).filter(Server.minion_status.contains("Up"), Server.month_year==month_year)
    pkg_objs = []
    for i in fdom_packages:
        server_id = i.id
        pkgs = session.query(Package).filter(Package.server_id == server_id)
        pkg_name_id = {pkg.name:pkg.id for pkg in pkgs}
        pkg_list = [pkg for pkg in pkg_name_id.keys()]
        try:
            pkg_current_vers = local.cmd(f"{i.name}", 'pkg.version', [x for x in pkg_list])
            if bool(pkg_current_vers[i.name]):
                if isinstance(pkg_current_vers[i.name], dict):
                    for pkg_name, curr_vers in pkg_current_vers[i.name].items():
                        if ',' in curr_vers:
                            curr_vers = curr_vers.split(',')
                            sorted_lst = sorted(curr_vers, key=lambda x: int("".join([i for i in x if i.isdigit()])))
                            curr_vers = sorted_lst[-1]
                        pkg_objs.append(dict(id=pkg_name_id[pkg_name], current_version= curr_vers))
        except:
            print(f'{i.name} has no connection')

    return pkg_objs


