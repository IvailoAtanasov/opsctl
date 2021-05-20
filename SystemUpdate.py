#!/usr/bin/env python3
import sys
sys.path.insert(1, '/opt/susemgr-scripts/suse-mgr-pkg')
from patching import *
from channels import *
import salt.client
sys.path.insert(1, '/opt/susemgr-scripts')
import smtools
import argparse
from argparse import RawTextHelpFormatter
import time
from interruptingcow import timeout
import logging
sys.path.insert(1, '/home/BSINFRAPRD/iatanasov/reporting_project/db_py')
from base import Session
from sqlalchemy import MetaData
from base import Server

## CLI
parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter,description=('''\
         Usage:
         SystemUpdate.py -s <server name>

               '''))
parser.add_argument("-s", "--server", help="Provide server short name")
args = parser.parse_args()
##

## Configurations
salt_client = salt.client.LocalClient()

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',datefmt='%m/%d/%Y %H:%M:%S',filename=f'/var/log/ops/{args.server}.log', level=logging.INFO)

if  salt_client.cmd(args.server, 'test.ping', []):
    logging.info(f'{args.server} salt minion connection successful')
else:
    logging.critical(f'minion on {args.server} is not working')
    sys.exit(100)
try:
    (client,session)=smtools.suman_login()
    logging.info(f'{args.server} Login to suse-manager successful')
    system_id = get_system_id(args.server, client, session)
    subscribed_base_channel = get_subscribed_base_channel(args.server)
    subscribed_child_channels = get_subscribed_child_channels(args.server)
    logging.info(f'{args.server} fetching data from suse-manager successful')

except:
    logging.critical(f'{args.server} has no connection to SUSE-MANAGER')
    sys.exit(101)
##
## Prepare system
if stop_reboot_protectior(args.server): 
    logging.info(f'{args.server} reboot protector is disabled')
else:
    logging.critical(f'{args.server} reboot protector cannot be stoped')
    sys.exit(102)
try:
    os = get_system_os_version(salt_client,args.server)
except:
    logging.critical(f'{args.server} os release cannot be retrieved')
    sys.exit(103)
correct_base_channel = get_correct_base_channel(os, subscribed_base_channel)
cluster = is_cluster(args.server)
exception = is_exception_update(args.server)
## Get initial kernel version
initial_kernel = get_current_kernel(args.server)
##

if not cluster:
    if not exception:
        ## Configure channels
        if subscribed_base_channel != correct_base_channel:
            logging.error(f'{args.server} current subscribed channel is not correct and will be changed')
            try:
                set_base_channel(args.server, correct_base_channel)
                logging.info(f'{args.server} {correct_base_channel} has been subscribed on {args.server}')
            except:
                logging.critical(f'{args.server} {correct_base_channel} cannot be subscribed')
                sys.exit(104)
            set_child_channels(args.server, [])
            subscribable_child_channels =  get_subscribable_child_channels(args.server)
            subscribed_base_channel = get_subscribed_base_channel(args.server)
            target_child_channels = get_target_child_channels(subscribable_child_channels, subscribed_base_channel, 'fdom')
            try:
                set_child_channels(args.server, target_child_channels)
                logging.info(f'{args.server} FDOM child channels has been subscribed')
            except:
                logging.critical(f'{args.server} FDOM child channels cannot be subscribed')
                sys.exit(105)
        elif not  any('fdom' in string for string in subscribed_child_channels):
            logging.info(f'{args.server} current base channel match the correct one')
            logging.error(f'{args.server} fdom child channels are not subscribed')
            set_child_channels(args.server, [])
            subscribable_child_channels =  get_subscribable_child_channels(args.server)
            subscribed_base_channel = get_subscribed_base_channel(args.server)
            target_child_channels = get_target_child_channels(subscribable_child_channels, subscribed_base_channel, 'fdom')
            try:
                set_child_channels(args.server, target_child_channels)
                logging.info(f'{args.server} FDOM child channels has been subscribed')
            except:
                logging.critical(f'{args.server} FDOM child channels cannot be subscribed')
                sys.exit(106)
        else:
            logging.info(f'{args.server} base and child channels are correct')
        ## Refresh repos
        ref = salt_client.cmd(args.server, 'state.apply', [])
        # uncomment for debug logging.info(f'{ref}')
        ##
        ## Insert initial packages in lxinfra02 db
        try:
            update_current_packages(args.server)
        except:
            logging.info(f'{args.server} missing from lxinfra DB')
        ##
        ## Update
        try:
            patch_res = salt_client.cmd(args.server, 'pkg.upgrade', [])
            if not bool(patch_res.get(args.server)):
                logging.info('No packages has been updated')
                sys.exit(140)
            logging.info(f'{args.server} update command has been executed')
            logging.info(f'{patch_res}')
        except:
            logging.critical(f'{args.server} update command failed')
        ##
        ## Check for sp migrate and if yes prepare syste,
        if for_sp_mig(args.server):
            try:
                sp_grains = salt_client.cmd(args.server, 'grains.item', ['sp_mig_target_base', 'sp_mig_target_child'])
                if not sp_grains:
                    logging.critical(f'{args.server} sp migrate targets cannot be retrieved')
            except:
                sys.exit(107)
            for k,v in sp_grains.items():
                if isinstance(v, dict):
                    sp_mig_base = v['sp_mig_target_base'][-1]
                    sp_mig_child = v['sp_mig_target_child'][-1]
            try:
                set_base_channel(args.server, sp_mig_base)
                subscribed_base_channel = get_subscribed_base_channel(args.server)
                set_child_channels(args.server, [])
                subscribable_child_channels = get_subscribable_child_channels(args.server)
                target_child_channels = get_target_child_channels(subscribable_child_channels, subscribed_base_channel, sp_mig_child)
                set_child_channels(args.server, target_child_channels)
                logging.info(f'{args.server} sp migration base channel {sp_mig_base} has been  subscribed successfully')
                logging.info(f'{args.server} sp migration child channels {sp_mig_child} have been subscribed successfully')
            except:
                logging.critical('{args.server} sp migration channels cannot be subscribed')
                sys.exit(108)
            time.sleep(120)
            state_apply = salt_client.cmd(args.server, 'state.highstate', [])
            print(state_apply)
            try:
                update_current_packages(args.server)
            except:
                logging.info(f'{args.server} missing from lxinfra DB')
            try:
                upgrade_res = salt_client.cmd(args.server, 'cmd.shell', ['zypper dup -y --download-in-advance --allow-vendor-change'])
                logging.info(f'{upgrade_res}')
                logging.info(f'{args.server} upgraded successfully')
            except:
                logging.critical(f'{args.server} upgrade command failed')
                sys.exit(109)
            ##
        ## Reboot and check when system is up again    
        salt_client.cmd(args.server, 'cmd.run',['reboot'])
        time.sleep(30)
        logging.info(f'{args.server} reboot command executed')
        boot_result = False
        try:
            with timeout(60 * 45, exception=RuntimeError):
                while not boot_result: 
                    res =  salt_client.cmd(args.server, 'test.ping', [])[args.server]
                    if res:
                        boot_result = True
                    if not boot_result:
                        time.sleep(240)
        except RuntimeError:
            logging.critical(f'{args.server} System is not booting more than 45 minutes')
            sys.exit(110)
        logging.info(f'{args.server} is up after reboot')
        ##
        #After patching check             
        try:
            status = get_patch_status(args.server)
            if status is None:
                logging.info(f'{args.server} status command do not return result')
                sys.exit(130)
            logging.info(f'{args.server} status is {status}')
        except:
            logging.info(f'{args.server} missing from lxinfra02 DB')
            sys.exit(120)
        #update db status
        if 'Missing' in status:
            result_kernel = get_current_kernel(args.server)
            if result_kernel == initial_kernel:
                logging.critical('kernel is not patched')
                sys.exit(150)
        elif status == 'Not Patched':
            logging.critical(f'{args.server} current kernel version do not match target kernel version')
            sys.exit(111)
        elif status == 'Over Patched':
            logging.critical(f'{args.server} current kernel version is overpatched')
            sys.exit(160)
        else:
            insert_packet_results(args.server)
            insert_patching_result(args.server)
            sys.exit(0)
    else:
        logging.error(f'{args.server} is in exception list /opt/susemgr-scripts/exclude4patch ')
else:
    logging.error(f'{args.server} is cluster node and will not be updated')
    sys.exit(112)

