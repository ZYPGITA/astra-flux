# -*- coding: utf-8 -*-
import time

from astraflux import *


class RpcFunction(ServiceConstructor):
    service_name = 'test_server'

    @rpc_decorator
    def get_service_version(self):
        return {"service_version": self.version}

    @rpc_decorator
    def test_func(self, **args):
        return args


def aa(x):
    print(x)
    print(ipaddr())


class WorkerFunction(WorkerConstructor):
    worker_name = 'test_server'

    def run(self, data):
        self.logger.info(data)

        o = process_executor()
        o.submit(func=aa, x=1111)
        o.submit(func=aa, x=2222)
        o.start()

        time.sleep(15)
        self.logger.info(f"worker done {data['task_id']}")
