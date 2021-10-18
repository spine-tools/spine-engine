######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for RemoteSpineServiceImpl class.
:author: P. Pääkkönen (VTT)
:date:   18.8.2021
"""

import unittest
from unittest.mock import NonCallableMagicMock

import sys 
#sys.path.append('./../../spine_engine/server')
#sys.path.append('./../spine_engine')
#sys.path.append('./../../..')

from pathlib import Path
import os
from shutil import copyfile,rmtree
from zipfile import ZipFile

#from RemoteSpineServiceImpl import RemoteSpineServiceImpl
from spine_engine.server.RemoteSpineServiceImpl import RemoteSpineServiceImpl

class TestRemoteSpineServiceImpl(unittest.TestCase):

    def setUp(self):
        #if os.path.exists('./tests/server/helloworld')==False:
        if os.path.exists(os.path.join(str(Path(__file__).parent.parent.parent),'hellow'))==False:
            #print("helloworld2 path doesn't exist.")
            #os.makedirs('./tests/server/helloworld')
            os.makedirs(os.path.join(str(Path(__file__).parent.parent.parent),'hellow'))
            #print("created %s folder"%os.path.join(str(Path(__file__).parent.parent.parent),'hellow'))
        #copyfile("./tests/server/test_zipfile.zip","./tests/server/helloworld/test_zipfile.zip")
        copyfile(os.path.join(str(Path(__file__).parent),"test_zipfile.zip"),os.path.join(str(Path(__file__).parent.parent.parent),"hellow","test_zipfile.zip"))
        #print("copied test_zipfile.zip to ./helloworld")
        with ZipFile(os.path.join(str(Path(__file__).parent.parent.parent),"hellow","test_zipfile.zip"), 'r') as zipObj:
            zipObj.extractall(os.path.join(str(Path(__file__).parent.parent.parent),'hellow'))
            #zipObj.extractall('./tests/server/helloworld')
            #print("extracted ZIP-file to %s"%os.path.join(str(Path(__file__).parent.parent.parent),'hellow'))

    @classmethod
    def tearDownClass(cls):
        rmtree(os.path.join(str(Path(__file__).parent.parent.parent),'hellow'))
        #print("deleted %s"%os.path.join(str(Path(__file__).parent.parent.parent),'hellow'))
        #rmtree('./tests/server/helloworld')
        #print("deleted ./tests/server/helloworld")


    @staticmethod
    def _mock_data(
        items, connections, node_successors,
          execution_permits,specifications,settings,
          project_dir
    ):
        """Returns mock data to be passed to the class.
        Args:
            items (list(dict)): See SpineEngine.__init()
            connections (list of dict): See SpineEngine.__init()
            node_successors (dict(str,list(str))): See SpineEngine.__init()
            execution_permits (dict(str,bool)): See SpineEngine.__init()
            specifications (dict(str,list(dict))): SpineEngine.__init()
            settings (dict): SpineEngine.__init()
            project_dir (str): SpineEngine.__init()
        Returns:
            NonCallableMagicMock
        """
        item = NonCallableMagicMock()
        item.items=items
        item.connections=connections
        item.node_successors=node_successors
        item.execution_permits=execution_permits
        item.specifications=specifications
        item.settings=settings
        item.project_dir=project_dir
        return item


    @staticmethod
    def _dict_data(
        items, connections, node_successors,
          execution_permits,specifications,settings,
          project_dir,jumps,items_module_name
    ):
        """Returns a dict to be passed to the class.
        Args:
            items (list(dict)): See SpineEngine.__init()
            connections (list of dict): See SpineEngine.__init()
            node_successors (dict(str,list(str))): See SpineEngine.__init()
            execution_permits (dict(str,bool)): See SpineEngine.__init()
            specifications (dict(str,list(dict))): SpineEngine.__init()
            settings (dict): SpineEngine.__init()
            project_dir (str): SpineEngine.__init()
            jumps (List of jump dicts): SpineEngine.__init()
            items_module_name (str): SpineEngine.__init()
        Returns:
            dict
        """
        item = dict()
        item['items']=items
        item['specifications']=specifications
        item['connections']=connections
        item['jumps']=jumps
        item['node_successors']=node_successors
        item['execution_permits']=execution_permits
        item['items_module_name']=items_module_name
        item['settings']=settings
        item['project_dir']=project_dir
        return item



    def test_basic_service_call_succeeds(self):
        """Tests execution with all data items present"""
        
        dict_data = self._dict_data(items={'helloworld': {'type': 'Tool', 'description': '', 'x': -91.6640625,
            'y': -5.609375, 'specification': 'helloworld2', 'execute_in_work': True, 'cmd_line_args': []},
            'Data Connection 1': {'type': 'Data Connection', 'description': '', 'x': 62.7109375, 'y': 8.609375,
             'references': [{'type': 'path', 'relative': True, 'path': 'input2.txt'}]}},
            #'references': []}},
            connections=[{'from': ['Data Connection 1', 'left'], 'to': ['helloworld', 'right']}],
            node_successors={'Data Connection 1': ['helloworld'], 'helloworld': []},
            execution_permits={'Data Connection 1': True, 'helloworld': True},
            project_dir = '/helloworld',
            specifications = {'Tool': [{'name': 'helloworld2', 'tooltype': 'python', 
            'includes': ['helloworld.py'], 'description': '', 'inputfiles': ['input2.txt'], 
            'inputfiles_opt': [], 'outputfiles': [], 'cmdline_args': [], 'execute_in_work': True, 
            'includes_main_path': '../../..', 
            'definition_file_path': 
            '/home/ubuntu/sw/spine/helloworld/.spinetoolbox/specifications/Tool/helloworld2.json'}]},
            settings = {'appSettings/previousProject': '/home/ubuntu/sw/spine/helloworld', 
            #settings = {'appSettings/previousProject': '',
            'appSettings/recentProjectStorages': '/home/ubuntu/sw/spine', 
            'appSettings/recentProjects': 'helloworld<>/home/ubuntu/sw/spine/helloworld', 
            'appSettings/showExitPrompt': '2', 
            'appSettings/toolbarIconOrdering': 
            'Importer;;View;;Tool;;Data Connection;;Data Transformer;;Gimlet;;Exporter;;Data Store', 
            'appSettings/workDir': '/home/ubuntu/sw/spine/Spine-Toolbox/work'},
            jumps=[],
            items_module_name= 'spine_items')

        dict_data2 = self._dict_data(items={'helloworld': {'type': 'Tool', 'description': '', 'x': -91.6640625,
            'y': -5.609375, 'specification': 'helloworld2', 'execute_in_work': False, 'cmd_line_args': []},
            'Data Connection 1': {'type': 'Data Connection', 'description': '', 'x': 62.7109375, 'y': 8.609375,
            'references': [{'type': 'path', 'relative': True, 'path': 'input2.txt'}]}},
            #'references': []}},
            connections=[{'from': ['Data Connection 1', 'left'], 'to': ['helloworld', 'right']}],
            node_successors={'Data Connection 1': ['helloworld'], 'helloworld': []},
            execution_permits={'Data Connection 1': True, 'helloworld': True},
            project_dir = './hellow',
            #project_dir = '/home/ubuntu/sw/spine/helloworld',
            specifications = {'Tool': [{'name': 'helloworld2', 'tooltype': 'python',
            'includes': ['helloworld.py'], 'description': '', 'inputfiles': ['input2.txt'],
            'inputfiles_opt': [], 'outputfiles': [], 'cmdline_args': [], 'execute_in_work': True,
            'includes_main_path': '../../..',
            'definition_file_path':
            './hellow/.spinetoolbox/specifications/Tool/helloworld2.json'}]},
            settings = {'appSettings/previousProject': './hellow',
            'appSettings/recentProjectStorages': './',
            'appSettings/recentProjects': 'helloworld<>./hellow',
            'appSettings/showExitPrompt': '2',
            'appSettings/toolbarIconOrdering':
            'Importer;;View;;Tool;;Data Connection;;Data Transformer;;Gimlet;;Exporter;;Data Store',
            'appSettings/workDir': './Spine-Toolbox/work'},
            jumps=[],
            items_module_name= 'spine_items')

        impl=RemoteSpineServiceImpl()
        #print("test_basic_service_call_succeeds(): input data to spine engine impl:")
        #print(dict_data2)
        eventData=impl.execute(dict_data2)
        #print("\ntest_basic_service_call_succeeds(): event+data:\n%s"%eventData)

        #asserts
        #self.assertEqual(len(eventData),30)
        #print("test_basic_service_call_succeeds() Final data value: %s"%eventData[len(eventData)-1][1])
        self.assertEqual(eventData[len(eventData)-1][1],"COMPLETED")

        #print("size of returned data: %d"%len(eventData))
        #check for returned data contents
        for i in eventData:
          self.assertNotEqual(i[0],None)
          self.assertNotEqual(i[1],None)
          self.assertNotEqual(len(i[0]),0)
          self.assertNotEqual(len(i[1]),0)
          #print("event: %s"%i[0])
          #print("data: %s"%i[1])


    def test_basic_service_call_succeeds_loop(self):
        """Tests execution with all data items present (in a loop)"""
        dict_data = self._dict_data(items={'helloworld': {'type': 'Tool', 'description': '', 'x': -91.6640625,
            'y': -5.609375, 'specification': 'helloworld2', 'execute_in_work': True, 'cmd_line_args': []},
            'Data Connection 1': {'type': 'Data Connection', 'description': '', 'x': 62.7109375, 'y': 8.609375,
             'references': [{'type': 'path', 'relative': True, 'path': 'input2.txt'}]}},
            connections=[{'from': ['Data Connection 1', 'left'], 'to': ['helloworld', 'right']}],
            node_successors={'Data Connection 1': ['helloworld'], 'helloworld': []},
            execution_permits={'Data Connection 1': True, 'helloworld': True},
            project_dir = './helloworld',
            specifications = {'Tool': [{'name': 'helloworld2', 'tooltype': 'python',
            'includes': ['helloworld.py'], 'description': '', 'inputfiles': ['input2.txt'],
            'inputfiles_opt': [], 'outputfiles': [], 'cmdline_args': [], 'execute_in_work': True,
            'includes_main_path': '../../..',
            'definition_file_path':
            '/home/ubuntu/sw/spine/helloworld/.spinetoolbox/specifications/Tool/helloworld2.json'}]},
            settings = {'appSettings/previousProject': '/home/ubuntu/sw/spine/helloworld',
            'appSettings/recentProjectStorages': '/home/ubuntu/sw/spine',
            'appSettings/recentProjects': 'helloworld<>/home/ubuntu/sw/spine/helloworld',
            'appSettings/showExitPrompt': '2',
            'appSettings/toolbarIconOrdering':
            'Importer;;View;;Tool;;Data Connection;;Data Transformer;;Gimlet;;Exporter;;Data Store',
            'appSettings/workDir': '/home/ubuntu/sw/spine/Spine-Toolbox/work'},
            jumps=[],
            items_module_name= 'spine_items')


        dict_data2 = self._dict_data(items={'helloworld': {'type': 'Tool', 'description': '', 'x': -91.6640625,
            'y': -5.609375, 'specification': 'helloworld2', 'execute_in_work': False, 'cmd_line_args': []},
            'Data Connection 1': {'type': 'Data Connection', 'description': '', 'x': 62.7109375, 'y': 8.609375,
             'references': [{'type': 'path', 'relative': True, 'path': 'input2.txt'}]}},
            connections=[{'from': ['Data Connection 1', 'left'], 'to': ['helloworld', 'right']}],
            node_successors={'Data Connection 1': ['helloworld'], 'helloworld': []},
            execution_permits={'Data Connection 1': True, 'helloworld': True},
            project_dir = './hellow',
            specifications = {'Tool': [{'name': 'helloworld2', 'tooltype': 'python',
            'includes': ['helloworld.py'], 'description': '', 'inputfiles': ['input2.txt'],
            'inputfiles_opt': [], 'outputfiles': [], 'cmdline_args': [], 'execute_in_work': True,
            'includes_main_path': '../../..',
            'definition_file_path':
            './hellow/.spinetoolbox/specifications/Tool/helloworld2.json'}]},
            settings = {'appSettings/previousProject': './hellow',
            'appSettings/recentProjectStorages': './',
            'appSettings/recentProjects': 'helloworld<>./hellow',
            'appSettings/showExitPrompt': '2',
            'appSettings/toolbarIconOrdering':
            'Importer;;View;;Tool;;Data Connection;;Data Transformer;;Gimlet;;Exporter;;Data Store',
            'appSettings/workDir': './Spine-Toolbox/work'},
            jumps=[],
            items_module_name= 'spine_items')

        impl=RemoteSpineServiceImpl()

        #loop periodic calls to the service
        i=0
        while i<3:
            #print("test_basic_service_call_succeeds_loop(): iteration: %d"%i)
            i=i+1
            eventData=impl.execute(dict_data2)

            #asserts
            #self.assertEqual(len(eventData),30)
            #print("test_basic_service_call_succeeds() Final data value: %s"%eventData[len(eventData)-1][1])
            #print("test_basic_service_call_succeeds_loop(): events+data:  %s"%eventData)
            self.assertEqual(eventData[len(eventData)-1][1],"COMPLETED")

            #print("size of returned data: %d"%len(eventData))
            #check for returned data contents
            for j in eventData:
                self.assertNotEqual(j[0],None)
                self.assertNotEqual(j[1],None)
                self.assertNotEqual(len(j[0]),0)
                self.assertNotEqual(len(j[1]),0)
                #print("event: %s"%i[0])
                #print("data: %s"%i[1])

            #time.sleep(1)


    def test_fail_invalid_projectdir(self):
        """Tests execution with an invalid project_dir in data"""
        dict_data = self._dict_data(items={'helloworld': {'type': 'Tool', 'description': '', 'x': -91.6640625,
            'y': -5.609375, 'specification': 'helloworld2', 'execute_in_work': False, 'cmd_line_args': []},
            'Data Connection 1': {'type': 'Data Connection', 'description': '', 'x': 62.7109375, 'y': 8.609375,
             'references': [{'type': 'path', 'relative': True, 'path': 'input2.txt'}]}},
            connections=[{'from': ['Data Connection 1', 'left'], 'to': ['helloworld', 'right']}],
            node_successors={'Data Connection 1': ['helloworld'], 'helloworld': []},
            execution_permits={'Data Connection 1': True, 'helloworld': True},
            project_dir = '/tests/server/helloworld3',
            specifications = {'Tool': [{'name': 'helloworld2', 'tooltype': 'python',
            'includes': ['helloworld.py'], 'description': '', 'inputfiles': ['input2.txt'],
            'inputfiles_opt': [], 'outputfiles': [], 'cmdline_args': [], 'execute_in_work': True,
            'includes_main_path': '../../..',
            'definition_file_path':
            '/home/ubuntu/sw/spine/helloworld/.spinetoolbox/specifications/Tool/helloworld2.json'}]},
            settings = {'appSettings/previousProject': '/home/ubuntu/sw/spine/helloworld',
            'appSettings/recentProjectStorages': '/home/ubuntu/sw/spine',
            'appSettings/recentProjects': 'helloworld<>/home/ubuntu/sw/spine/helloworld',
            'appSettings/showExitPrompt': '2',
            'appSettings/toolbarIconOrdering':
            'Importer;;View;;Tool;;Data Connection;;Data Transformer;;Gimlet;;Exporter;;Data Store',
            'appSettings/workDir': '/home/ubuntu/sw/spine/Spine-Toolbox/work'},
            jumps=[],
            items_module_name= 'spine_items')

        impl=RemoteSpineServiceImpl()
        eventData=impl.execute(dict_data)

        #asserts
        #self.assertEqual(len(eventData),17)
        #print("test_fail_invalid_projectdir() %s"%eventData)
        #print("test_fail_invalid_projectdir() Final data value: %s"%eventData[len(eventData)-1][1])
        self.assertEqual(eventData[len(eventData)-1][1],"FAILED")
        #print("size of returned data: %d"%len(eventData))
        #check for returned data contents
        for i in eventData:
          self.assertNotEqual(i[0],None)
          self.assertNotEqual(i[1],None)
          self.assertNotEqual(len(i[0]),0)
          self.assertNotEqual(len(i[1]),0)
          #print("event: %s"%i[0])
          #print("data: %s"%i[1])


    def test_missing_field_in_data(self):
        """Tests execution with a missing field in data (that an error is raised)"""
        dict_data = self._dict_data(items={'helloworld': {'type': 'Tool', 'description': '', 'x': -91.6640625,
            'y': -5.609375, 'specification': 'helloworld2', 'execute_in_work': False, 'cmd_line_args': []},
            'Data Connection 1': {'type': 'Data Connection', 'description': '', 'x': 62.7109375, 'y': 8.609375,
             'references': [{'type': 'path', 'relative': True, 'path': 'input2.txt'}]}},
            connections=[{'from': ['Data Connection 1', 'left'], 'to': ['helloworld', 'right']}],
            node_successors={'Data Connection 1': ['helloworld'], 'helloworld': []},
            project_dir = './hellow',
            execution_permits='',
            specifications = {'Tool': [{'name': 'helloworld2', 'tooltype': 'python',
            'includes': ['helloworld.py'], 'description': '', 'inputfiles': ['input2.txt'],
            'inputfiles_opt': [], 'outputfiles': [], 'cmdline_args': [], 'execute_in_work': True,
            'includes_main_path': '../../..',
            'definition_file_path':
            '/home/ubuntu/sw/spine/helloworld/.spinetoolbox/specifications/Tool/helloworld2.json'}]},
            settings = {'appSettings/previousProject': '/home/ubuntu/sw/spine/helloworld',
            'appSettings/recentProjectStorages': '/home/ubuntu/sw/spine',
            'appSettings/recentProjects': 'helloworld<>/home/ubuntu/sw/spine/helloworld',
            'appSettings/showExitPrompt': '2',
            'appSettings/toolbarIconOrdering':
            'Importer;;View;;Tool;;Data Connection;;Data Transformer;;Gimlet;;Exporter;;Data Store',
            'appSettings/workDir': '/home/ubuntu/sw/spine/Spine-Toolbox/work'},
            jumps=[],
            items_module_name= 'spine_items')

        impl=RemoteSpineServiceImpl()
        with self.assertRaises(ValueError):
            eventData=impl.execute(dict_data)
        
                

if __name__ == '__main__':
    unittest.main()
