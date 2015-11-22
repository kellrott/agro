#!/usr/bin/env python

import unittest
import uuid
import utilities
import time

from grpc.beta import implementations
from pyagro import agro_pb2


class TestAgroClient(utilities.ServerTest):

    def test_program(self):
        channel = implementations.insecure_channel('localhost', 9713)
        print "Connected"
        stub = agro_pb2.beta_create_Agro_stub(channel)
        
        
        stub = None
        