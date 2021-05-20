from paramiko import * 
from subprocess import Popen, PIPE

def salt_ssh(target, command):
    from salt.client.ssh.client import SSHClient
    client = SSHClient(c_path='/etc/salt/master')
    client.opts['ssh_skip_roster'] = True
    client.opts['raw_shell'] = True
    command = client.cmd(tgt=target, fun=command)
    server = next(iter(command))
    retcode =  command[server]['retcode']
    stdout = command[server]['stdout']
    stderr = command[server]['stderr']
    if retcode == 0:
        return stdout,retcode
    else:
        return stderr,retcode

def paramiko_ssh(target_list, command):
    res = []
    k = RSAKey.from_private_key_file("/root/.ssh/id_rsa")
    ssh_status = False
    c = SSHClient()
    c.set_missing_host_key_policy(AutoAddPolicy())
    for target in target_list:
        try:
            c.connect( hostname = target, username = "root", pkey = k )
            ssh_status = True
        except:
            return ssh_status, ''
        stdin , stdout, stderr = c.exec_command(command)
        res.append(dict(status=ssh_status, stdout=stdout.read().decode('utf8').strip()))
    return res

def ssh_status(target):
    status = False
    process = Popen([f'ssh -q -oStrictHostKeyChecking=no -oBatchMode=yes {target} pwd'], stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = process.communicate()
    if stdout.decode('utf8').strip():
        status = True
    return status

def ssh_cmd(target, cmd):
    process = Popen([f'ssh -q -oStrictHostKeyChecking=no -oBatchMode=yes {target} {cmd}'], stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = process.communicate()
    return stdout.decode('utf8').strip(), stderr.decode('utf8').strip()








