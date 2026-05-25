# -*- coding: utf-8 -*-


from astraflux import *


class RpcFunction(ServiceConstructor):
    service_name = 'sub_test_server'

    @rpc_decorator
    def get_service_version(self):
        return {"service_version": self.version}

    @rpc_decorator
    def test_func(self, **args):
        return args


class WorkerFunction(WorkerConstructor):
    worker_name = 'sub_test_server'

    def run(self, data):
        self.logger.info(data)
        self.logger.info(f"worker done {data['task_id']}")

        self.logger.info(f'snowflake_id == {snowflake_id()}')
        self.logger.warning(f'snowflake_id == {devices_id()}')

        data = proxy_call(
            service_name='test_server',
            method_name='get_service_version',
        )
        self.logger.info(f'get_service_version == {data}')

        data = proxy_call(
            service_name='sub_test_server',
            method_name='test_func',
            x=1, y=2
        )
        self.logger.info(f'test_func == {data}')

        self.logger.info(f'ipaddr == {ipaddr()}')
