#!/usr/bin/env python

import unittest
import uuid
import utilities
import time

import pyagro

type_map = {
    int.__name__ : "int",
    str.__name__ : "str",
    unicode.__name__ : "str",
    bool.__name__ : "bool",
    list.__name__ : "list",
    dict.__name__ : "dict",
    long.__name__ : "int"
}

def compare_dicts(a, b):
    a_s = set(a.keys())
    b_s = set(b.keys())
    
    a_d = a_s.difference(b_s)
    b_d = b_s.difference(a_s)
    #print a
    #print b
    assert(len(a_d) == 0 or a_d == set(['_id']))
    assert(len(b_d) == 0 or b_d == set(['_id']))
    
    for k in a_s:
        assert( type_map[type(a[k]).__name__] == type_map[type(b[k]).__name__] )
        if isinstance(a[k], dict):
            compare_dicts(a[k], b[k])
        if isinstance(a[k], list):
            assert(len(a[k]) == len(b[k]))
            for a_k, b_k in zip(a[k],b[k]):
                if isinstance(a_k, dict):
                    compare_dicts(a_k, b_k)
        
class TestMapPack(utilities.ServerTest):
    
    def test_pack_1(self):
        start_map = {
            "a" : [
                {'item' : 1},
                {'item' : 2},
                {'item' : 3}
            ]
        }
        doc = pyagro.pack_doc('test_id', start_map)
        #for f in doc.fields:
        #    print f
        end_map = pyagro.unpack_doc(doc)
        print
        print start_map
        print doc
        print end_map
        compare_dicts(start_map, end_map)
    
    def test_pack_2(self):
        start_map = {
            "a" : "a",
            "b" : [1,2,3],
            "c" : True,
            "d" : {
                "da" : ["hi", "there"]
            },
            "e" : [
                {"item":"a"},
                {"item":"b"}
            ],
            "f" : {
                "a" : {
                    "a1" : 1
                },
                "b" : {
                    "b2"  : 2
                }
            }
        }
        doc = pyagro.pack_doc('test_id', start_map)
        #for f in doc.fields:
        #    print f
        end_map = pyagro.unpack_doc(doc)
        compare_dicts(start_map, end_map)
    
    def test_pack_pipeline(self):
        pipeline = {
            "a_galaxy_workflow": "true",
            "annotation": "",
            "format-version": "0.1",
            "name": "WorkflowTest",
            "steps": {
                "0": {
                    "annotation": "",
                    "id": 0,
                    "input_connections": {},
                    "inputs": [
                        {
                            "description": "",
                            "name": "input_file_1"
                        }
                    ],
                    "label": "input_file_1",
                    "name": "Input dataset",
                    "outputs": [],
                    "position": {
                        "left": 197,
                        "top": 199
                    },
                    "tool_errors": None,
                    "tool_id": None,
                    "tool_state": "{\"name\": \"input_file_1\"}",
                    "tool_version": None,
                    "type": "data_input",
                    "user_outputs": [],
                    "uuid": "114e8769-4229-4406-80e9-278fa19d66a0"
                },
                "1": {
                    "annotation": "",
                    "id": 1,
                    "input_connections": {},
                    "inputs": [
                        {
                            "description": "",
                            "name": "input_file_2"
                        }
                    ],
                    "label": "input_file_2",
                    "name": "Input dataset",
                    "outputs": [],
                    "position": {
                        "left": 192,
                        "top": 406
                    },
                    "tool_errors": None,
                    "tool_id": None,
                    "tool_state": "{\"name\": \"input_file_2\"}",
                    "tool_version": None,
                    "type": "data_input",
                    "user_outputs": [],
                    "uuid": "73c793a2-01e9-4dc1-ae65-67e4d1a363e4"
                },
                "2": {
                    "annotation": "",
                    "id": 2,
                    "input_connections": {
                        "input": {
                            "id": 0,
                            "output_name": "output"
                        }
                    },
                    "inputs": [],
                    "label": None,
                    "name": "Select first",
                    "outputs": [
                        {
                            "name": "out_file1",
                            "type": "input"
                        }
                    ],
                    "position": {
                        "left": 435,
                        "top": 200
                    }, 
                    "tool_errors": None,
                    "tool_id": "Show beginning1",
                    "tool_state": "{\"__page__\": 0, \"input\": \"None\", \"__rerun_remap_job_id__\": None, \"lineNum\": \"\\\"1\\\"\"}",
                    "tool_version": "1.0.0",
                    "type": "tool",
                    "user_outputs": [],
                    "uuid": "96e1c252-ac47-4d28-993d-6b4d1e8c922a"
                },
                "3": {
                    "annotation": "tail_select",
                    "id": 3,
                    "input_connections": {
                        "input": {
                            "id": 1,
                            "output_name": "output"
                        }
                    },
                    "inputs": [
                        {
                            "description": "runtime parameter for tool Select last",
                            "name": "lineNum"
                        }
                    ],
                    "label": None,
                    "name": "Select last",
                    "outputs": [
                        {
                            "name": "out_file1",
                            "type": "input"
                        }
                    ],
                    "position": {
                        "left": 440,
                        "top": 410
                    },
                    "post_job_actions": {
                        "HideDatasetActionout_file1": {
                            "action_arguments": {},
                            "action_type": "HideDatasetAction",
                            "output_name": "out_file1"
                        }
                    },
                    "tool_errors": None,
                    "tool_id": "Show tail1",
                    "tool_state": "{\"__page__\": 0, \"input\": \"None\", \"__rerun_remap_job_id__\": None, \"lineNum\": \"{\\\"__class__\\\": \\\"RuntimeValue\\\"}\"}",
                    "tool_version": "1.0.0",
                    "type": "tool",
                    "user_outputs": [],
                    "uuid": "5d7f9a1e-bc12-43b8-80f0-829aa18a2e66"
                },
                "4": {
                    "annotation": "concat_out",
                    "id": 4,
                    "input_connections": {
                        "input1": {
                            "id": 2,
                            "output_name": "out_file1"
                        },
                        "queries_0|input2": {
                            "id": 3,
                            "output_name": "out_file1"
                        }
                    },
                    "inputs": [],
                    "label": None,
                    "name": "Concatenate datasets",
                    "outputs": [
                        {
                            "name": "out_file1",
                            "type": "input"
                        }
                    ],
                    "position": {
                        "left": 662.5,
                        "top": 236.5
                    },
                    "post_job_actions": {
                        "RenameDatasetActionout_file1": {
                            "action_arguments": {
                                "newname": "concat_output"
                            },
                            "action_type": "RenameDatasetAction",
                            "output_name": "out_file1"
                        }
                    },
                    "tool_errors": None,
                    "tool_id": "cat1",
                    "tool_state": "{\"__page__\": 0, \"__rerun_remap_job_id__\": None, \"input1\": \"None\", \"queries\": \"[{\\\"input2\\\": None, \\\"__index__\\\": 0}]\"}",
                    "tool_version": "1.0.0",
                    "type": "tool",
                    "user_outputs": [],
                    "uuid": "656b8994-46e6-482f-b842-379427693985"
                }
            },
            "uuid": "e9948637-99ae-4507-b447-00fa4cde344f"
        }
        doc = pyagro.pack_doc('test_id', pipeline)
        #for f in doc.fields:
        #    print f
        end_map = pyagro.unpack_doc(doc)
        compare_dicts(pipeline, end_map)

        