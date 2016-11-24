"""
Copyright (c) 2016 Maciej Nabozny

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

from corecluster.agents.base_agent import BaseAgent
from corecluster.exceptions.agent import *
from corenetwork.utils.logger import log


class AgentThread(BaseAgent):
    storage = None
    task_type = 'storage'
    supported_actions = ['mount', 'umount']


    def task_error(self, task, exception):
        storage = task.get_obj('Storage')
        storage.set_state('locked')
        storage.save()
        super(AgentThread, self).task_error(task, exception)


    def task_finished(self, task):
        storage = task.get_obj('Storage')
        storage.save()
        super(AgentThread, self).task_finished(task)


    def task_failed(self, task, exception):
        storage = task.get_obj('Storage')
        storage.save()
        super(AgentThread, self).task_failed(task, exception)


    def mount(self, task):
        storage = task.get_obj('Storage')
        storage.state = 'ok'
        storage.save()


    def umount(self, task):
        storage = task.get_obj('Storage')
        storage.state = 'locked'
        storage.save()
