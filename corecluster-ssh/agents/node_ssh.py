"""
Copyright (c) 2014 Maciej Nabozny
              2015 Marta Nabozny

This file is part of CloudOver project.

CloudOver is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import libvirt
import os
import time
import subprocess

from corecluster.models.core.vm import VM
from corecluster.agents.base_agent import BaseAgent
from corecluster.exceptions.agent import *
from corenetwork.utils.logger import log
from corenetwork.utils import system, config


class AgentThread(BaseAgent):
    node = None
    task_type = 'node'
    supported_actions = ['load_image', 'delete', 'save_image', 'mount', 'umount', 'create_images_pool', 'check', 'suspend', 'wake_up']


    def load_image(self, task):
        node = task.get_obj('Node')
        node.check_online(task.ignore_errors)

        vm = task.get_obj('VM')
        image = task.get_obj('Image')

        if image.state != 'ok':
            raise TaskNotReady('image_wrong_state')

        system.call(['scp', image.storage.path + '/' + image.libvirt_name, '%s@%s:/images/%s' % (node.username, node.address, vm.id)])

        vm.set_state('stopped')
        vm.save()


    def delete(self, task):
        '''
        Delete volume
        '''
        node = task.get_obj('Node')
        node.check_online(task.ignore_errors)
        vm = task.get_obj('VM')
        if vm.state not in ['stopped', 'closed', 'closing'] and not task.ignore_errors:
            raise TaskNotReady('vm_not_stopped')

        system.call(['ssh', '-l', node.username, node.address, 'rm', '/images/' + str(vm.id)])


    def save_image(self, task):
        node = task.get_obj('Node')
        node.check_online(task.ignore_errors)

        vm = task.get_obj('VM')
        image = task.get_obj('Image')
        if not vm.in_state('stopped'):
            raise TaskNotReady('vm_not_stopped')

        vm.set_state('saving')
        vm.save()

        system.call(['scp', '%s@%s:/images/%s' % (node.username, node.address, vm.id), image.storage.path + '/' + image.libvirt_name])

        vm.set_state('stopped')
        vm.save()

        image.state = long(subprocess.check_output("qemu-img info " + image.storage.path + '/' + image.libvirt_name + " | grep 'virtual size'", shell=True).split()[3][1:])
        image.set_state('ok')
        image.save()


    def resize_image(self, task):
        vm = task.get_obj('VM')

        vm.node.check_online(task.ignore_errors)

        if not vm.in_state('stopped'):
            raise TaskNotReady('vm_not_stopped')

        system.call(['ssh', '-l', vm.node.username, vm.node.address, 'qemu-img', 'resize' '/images/%s' % vm.id, str(task.get_prop('size'))])


    def mount(self, task):
        pass


    def umount(self, task):
        node = task.get_obj('Node')
        node.state = 'offline'
        node.save()


    def create_images_pool(self, task):
        node = task.get_obj('Node')
        system.call(['ssh', '-l', node.username, node.address, 'mkdir', '/images'])


    def check(self, task):
        node = task.get_obj('Node')
        conn = node.libvirt_conn()

        for vm in node.vm_set.filter(state__in=['running', 'starting']):
            try:
                libvirt_vm = conn.lookupByName(vm.libvirt_name)
            except Exception as e:
                vm.set_state('stopped')
                vm.save()
                log(msg='Failed to find VM %s at node %s' % (vm.id, vm.node.address), exception=e, tags=('agent', 'node', 'error'), context=task.logger_ctx)

            if libvirt_vm.state() == libvirt.VIR_DOMAIN_RUNNING:
                vm.set_state('running')
                vm.save()
            else:
                vm.set_state('stopped')
                vm.save()
        conn.close()

        node.state = 'ok'
        node.save()


    def suspend(self, task):
        """
        Suspend node to RAM for defined in config seconds. After this time + NODE_WAKEUP_TIME
        node is suspended again, unles it's state is not wake up. Available only
        in admin site or through plugins.
        """
        node = task.get_obj('Node')

        if VM.objects.filter(node=node).exclude(state='closed').count() > 0:
            task.comment = "Node is in use. Aborting suspend"
            task.save()
            return

        node.set_state('suspend')
        node.save()

        log(msg="suspending node %s" % node.address, tags=('agent', 'node', 'info'), context=task.logger_ctx)
        system.call(['ping', '-c', '1', node.address])

        arp = open('/proc/net/arp', 'r').readlines()
        for line in arp:
            fields = line.split()
            if fields[0] == node.address:
                node.set_prop('mac', fields[3])

        node.save()

        conn = node.libvirt_conn()
        conn.suspendForDuration(libvirt.VIR_NODE_SUSPEND_TARGET_MEM, config.get('core', 'NODE_SUSPEND_DURATION'))
        conn.close()


    def wake_up(self, task):
        node = task.get_obj('Node')
        if node.has_prop('mac'):
            system.call(['wakeonlan', node.get_prop('mac')])
            if node.in_state('suspend'):
                time.sleep(config.get('core', 'NODE_WAKEUP_TIME'))
                node.start()
        else:
            raise TaskError('Cannot find node\'s MAC')
