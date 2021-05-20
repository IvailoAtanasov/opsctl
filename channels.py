import sys
sys.path.insert(1, '/opt/susemgr-scripts/suse-mgr-pkg')
sys.path.insert(1, '/opt/susemgr-scripts')
import smtools
from general import *
(client,session)=smtools.suman_login()
def get_system_id(hostname, client, session):
    # Get the system_id of the system. This ID will be used in all operations against susemanager.
    try:
       system_ids= client.system.getId(session,hostname.lower())
    except xmlrpc.client.Fault as e:
       smtools.fatal_error("Unable to connect SUSE Manager server to get system id")
       try:
           system_ids= client.system.getId(session,hostname.upper())
       except xmlrpc.client.Fault as e:
           smtools.fatal_error("Unable to connect SUSE Manager server to get system id")
    try:
       system_id = system_ids[0].get('id')
    except:
       print('System id not fount')
       exit()

    return int(system_id)

def get_subscribable_base_channels(hostname):
    base_channels_label = []
    system_id = get_system_id(hostname, client, session)
    base_channels = client.system.listBaseChannels(session, system_id)
    for i in base_channels:
            base_channels_label.append(i.get('label'))
    return base_channels_label

def get_subscribed_base_channel(hostname):
    system_id = get_system_id(hostname, client, session)
    cur_base_chan = client.system.getSubscribedBaseChannel(session,system_id).get('label')
    return cur_base_chan

def set_base_channel(hostname, basechannel):
    system_id = get_system_id(hostname, client, session)
    try:
        client.system.setBaseChannel(session, system_id, basechannel)
    except:
        print('Provided base channel is not a valid option')

    
def get_subscribable_child_channels(hostname):
    system_id = get_system_id(hostname, client, session)
    channels_label = []
    subscribable_child_channels = client.system.listSubscribableChildChannels(session,system_id)
    for i in subscribable_child_channels:
            channels_label.append(i.get('label'))
    return channels_label

def get_subscribed_child_channels(hostname):
    cur_child_chans = []
    system_id = get_system_id(hostname, client, session)
    s_child_channels = client.system.listSubscribedChildChannels(session, system_id)
    for i in s_child_channels:
            cur_child_chans.append(i.get('label'))
    return cur_child_chans

def set_child_channels(hostname, target_child_channels):
    system_id = get_system_id(hostname, client, session)
    client.system.setChildChannels(session, system_id, target_child_channels)

def get_correct_base_channel(os, current_system_base_channel):
    os_vendor = os.split(' ')[0]
    os_version = os.split(' ')[1]
    if 'suse' in os_vendor.lower():
        try:
            major_release = os_version.split('.')[0]
            service_pack = os_version.split('.')[1]
        except:
            major_release = os_version
            service_pack = 0
        if 'sap' in current_system_base_channel:
            target_base_channel = f'sles{major_release}sapsp{service_pack}'
        else:
            target_base_channel = f'sles{major_release}sp{service_pack}'
    else:
        if '7' in os_version:
            target_base_channel = 'rhel7-x86_64'
        elif '8' in os_version:
            target_base_channel = 'rhel8-x86_64'
        else:
            target_base_channel = 'redhat-6-x86_64'

    return target_base_channel

def get_target_child_channels(subscribable_child_channels, subscribed_base_channel, child_channel):#child channel is user input i.e. fdom
    target_child_channels = []
    for repo in subscribable_child_channels:
        #SLES11
        if '11' in subscribed_base_channel:
            repo_list = [child_channel, 'pool', 'sles11sp4-in']
            if any(substring in repo for substring in repo_list):
                    target_child_channels.append(repo)
        #RHEL
        elif 'rhel' in subscribed_base_channel or 'redhat' in subscribed_base_channel:
            if '7' in subscribed_base_channel:
                repo_list = ['cust','rhel7-in-x86_64-all', 'rhel7-res7-suse-manager-tools-x86_64', 'rhel7-in-dvd-suse-rhel7.1-x86_64', 'rhel7-in-x86_64-rhel7']
                if child_channel in repo or any(substring == repo for substring in repo_list):
                    target_child_channels.append(repo)

            elif '6' in subscribed_base_channel:
                if child_channel in repo or repo.startswith('redhat-6-pool-x86_64'):
                    target_child_channels.append(repo)

            else:
                repo_list = [child_channel, 'dvd', 'pool']
                if any(substring in repo for substring in repo_list):
                    target_child_channels.append(repo)

        #SLES12/15
        else:
            if child_channel in repo or 'pool' in repo:
                target_child_channels.append(repo)

    return target_child_channels

def get_system_os_version(salt_client, salt_minion):
    try:
        s = salt_client.cmd(salt_minion, 'grains.item', ['os_family', 'osrelease'])
    except:
        print(f'Minion on {salt_minion} is not responsible!')
    for k, v in s.items():
        if isinstance(v, dict):
            os_release  = f'{v.get("os_family")} {v.get("osrelease")}'
    return os_release

def get_system_os_ssh(target):
    release = ''
    os_type = ''
    os_release = ''
    out, retcode = salt_ssh(target, 'cat "/etc/*release"')
    result = out.split('\n')
    for row in result:
        if 'name' in row.lower()  and 'red' in row.lower():
            os_type = 'RedHat'
        elif 'version_id' in row.lower():
            release = row.split('=')[-1].replace('"','')
        elif 'name' in row.lower() and 'sles' in row.lower():
            os_type = 'Suse'
        elif 'version_id' in row.lower():
            release = row.split('=')[-1].replace('"', '')
        elif 'red' in row.lower() and '6.10' in row.lower():
            os_type = 'RedHat'
            release = '6.10'
            

    return f"{os_type} {release}"

   
