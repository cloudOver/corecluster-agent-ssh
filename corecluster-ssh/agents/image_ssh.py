"""
Copyright (c) 2014-2016 Maciej Nabozny
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

import base64
import libvirt
import urllib2
import os
from corecluster.agents.base_agent import BaseAgent
from corecluster.models.core import Device
from corecluster.exceptions.agent import TaskError, TaskFatalError
from corecluster.cache.data_chunk import DataChunk
from corenetwork.utils import system
from corenetwork.utils.logger import log


class AgentThread(BaseAgent):
    task_type = 'image'
    supported_actions = ['create', 'upload_url', 'upload_data', 'delete', 'attach', 'detach']
    lock_on_fail = ['create', 'upload_url', 'upload_data', 'delete', 'duplicate']

    def task_failed(self, task, exception):
        if task.action in self.lock_on_fail:
            image = task.get_obj('Image')
            image.set_state('failed')
            image.save()

        super(AgentThread, self).task_failed(task, exception)


    def create(self, task):
        image = task.get_obj('Image')
        system.call(['qemu-img', 'create', '-f', image.format, image.storage.path + '/' + image.libvirt_name, str(image.size)])
        image.size = os.stat(image.storage.path + '/' + image.libvirt_name).st_size
        image.set_state('ok')
        image.save()



    def upload_url(self, task):
        '''
        Download datq from url and put its contents into given image. Operation.data
        should contains:
        - action
        - url
        - size
        '''
        image = task.get_obj('Image')
        if image.attached_to != None:
            raise TaskError('image_attached')

        image.set_state('downloading')
        image.save()

        try:
            volume = open(image.storage.path + '/' + image.libvirt_name, 'r+')
        except Exception as e:
            raise TaskFatalError('libvirt_image_not_found', exception=e)

        try:
            remote = urllib2.urlopen(task.get_prop('url'))
        except Exception as e:
            raise TaskError('url_not_found', exception=e)

        bytes = 0
        while bytes < int(task.get_prop('size')):
            data = remote.read(1024*250)
            if len(data) == 0:
                break
            volume.write(data)
            bytes += len(data)
            image = task.get_obj('Image')
            image.set_prop('progress', float(bytes)/float(task.get_prop('size')))
            image.save()

        remote.close()
        volume.close()

        log(msg="Rebasing image to no backend", tags=('agent', 'image', 'info'), context=task.logger_ctx)
        if image.format in ['qcow2', 'qed']:
            r = system.call(['sudo',
                             'qemu-img', 'rebase',
                             '-u',
                             '-f', image.format,
                             '-u',
                             '-b', '',
                             image.storage.path + '/' + image.libvirt_name], stderr=None, stdout=None)
            if r != 0:
                image.set_state('failed')
                image.save()
                return

        image = task.get_obj('Image')
        image.size = os.stat(image.storage.path + '/' + image.libvirt_name).st_size
        image.set_state('ok')
        image.save()


    def upload_data(self, task):
        '''
        Put file given in operation.data['filename'] into given image (operation.image)
        at offset. The file can extend existing image. Operation.data should contain:
        - action
        - offset
        - filename
        '''
        image = task.get_obj('Image')
        if image.attached_to != None:
            raise TaskError('image_attached')

        image.set_state('downloading')
        image.save()

        try:
            volume = open(image.storage.path + '/' + image.libvirt_name, 'r+')
        except Exception as e:
            raise TaskFatalError('image_not_found', exception=e)

        data_chunk = DataChunk(cache_key=task.get_prop('chunk_id'))
        data = base64.b64decode(data_chunk.data)

        volume.seek(data_chunk.offset)
        volume.write(data)
        volume.close()

        data_chunk.delete()

        log(msg="Rebasing image to no backend", tags=('agent', 'image', 'info'), context=task.logger_ctx)
        if image.format in ['qcow2', 'qed']:
            r = system.call(['sudo',
                             'qemu-img', 'rebase',
                             '-u',
                             '-f', image.format,
                             '-u',
                             '-b', '',
                             image.storage.path + '/' + image.libvirt_name], stderr=None, stdout=None)
            if r != 0:
                image = task.get_obj('Image')
                image.set_state('failed')
                image.save()
                return

        image = task.get_obj('Image')
        image.size = os.stat(image.storage.path + '/' + image.libvirt_name).st_size
        image.set_state('ok')
        image.save()



    def delete(self, task):
        image = task.get_obj('Image')
        if image.attached_to != None and not image.attached_to.in_state('closed') and not task.ignore_errors:
            raise TaskError('image_attached')

        for vm in image.vm_set.all():
            if not vm.in_state('closed') and not task.ignore_errors:
                raise TaskError('image_attached')

        system.call(['rm', image.storage.path + '/' + image.libvirt_name])
        image.set_state('deleted')
        image.save()


    def attach(self, task):
        vm = task.get_obj('VM')

        vm.node.check_online(task.ignore_errors)

        image = task.get_obj('Image')
        conn = vm.node.libvirt_conn()

        if image.attached_to != None and not image.attached_to.in_state('closed'):
            raise TaskError('image_attached')

        if not vm.in_state('stopped'):
            raise TaskError('vm_not_stopped')

        if not image.in_state('ok'):
            raise TaskError('image_state')

        devices = [i.disk_dev for i in vm.image_set.all()]
        if 'device' in task.get_all_props().keys() and not int(task.get_prop('device')) in devices:
            disk_dev = int(task.get_prop('device'))
        else:
            disk_dev = 1
            while disk_dev in devices:
                disk_dev = disk_dev+1

        image.disk_dev = disk_dev
        image.attached_to = vm
        image.save()

        system.call(['scp', image.storage.path + '/' + image.libvirt_name,
                     '%s@%s:/images/permanent-%s' % (vm.node.username, vm.node.address, vm.id)])

        Device.create(image.id, vm, 'devices/image.xml', {'img': image, 'disk_dev': 'sd' + chr(ord('a')+disk_dev), 'vm': vm})

        vm.libvirt_redefine()

        conn.close()


    def detach(self, task):
        vm = task.get_obj('VM')

        vm.node.check_online(task.ignore_errors)

        image = task.get_obj('Image')

        conn = vm.node.libvirt_conn()
        if not vm.in_states(['stopped', 'closed']) and not task.ignore_errors:
            raise TaskError('vm_not_stopped')

        system.call(['scp', '%s@%s:/images/permanent-%s' % (vm.node.username, vm.node.address, vm.id),
                     image.storage.path + '/' + image.libvirt_name + '-tmp'])

        system.call(['mv', image.storage.path + '/' + image.libvirt_name + '-tmp', image.storage.path + '/' + image.libvirt_name])

        image.attached_to = None
        image.save()

        for device in Device.objects.filter(object_id=image.id).all():
            device.delete()

        try:
            vm.libvirt_redefine()
        except:
            pass

        conn.close()
