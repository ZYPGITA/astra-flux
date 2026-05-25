# -*- coding: utf-8 -*-

import os
import sys
import time
import secrets
import argparse
from collections import defaultdict
from datetime import datetime
from functools import wraps
from flask_cors import CORS
from flask import Flask, request, jsonify, session, send_from_directory, redirect, abort


from astraflux import AstraFlux
from astraflux.config.constants import *
from astraflux.exports.logger import logger
from astraflux.exports.other import config_obj


class WebApp:
    """Flask Web Application - AstraFlux Management Interface"""

    def __init__(self, config: dict):
        self.config = config
        self.logger = logger(dirname=PROJECT.NAME.value, filename=WEB.CONFIG.KEY.value)

        self.port = config[WEB.CONFIG.PORT.value]
        self.bind_ip = config[WEB.CONFIG.BIND_IP.value]
        self.username = config[WEB.CONFIG.USERNAME.value]
        self.password = config[WEB.CONFIG.PASSWORD.value]

        self.app = Flask(__name__, template_folder='views')
        # Read secret key from config, or generate a random one per process
        secret_key = config.get('secret_key', None)
        if secret_key:
            self.app.secret_key = secret_key
        else:
            self.app.secret_key = secrets.token_hex(32)
            self.logger.warning("No 'secret_key' in web config")
        self.route_prefix = '/api'

        self.views_dir = os.path.join(os.path.dirname(__file__), 'views')

        # Login rate limiting state
        self._login_attempts: dict = defaultdict(list)
        self._max_login_attempts = 5
        self._login_window_seconds = 300

        CORS(self.app, supports_credentials=True)

    @staticmethod
    def _login_required(f):
        """Decorator: require login for API endpoints."""

        @wraps(f)
        def decorated(*args, **kwargs):
            if not session.get('logged_in'):
                return jsonify({
                    'status': 'error',
                    'message': 'Unauthorized. Please login first.'
                }), 401
            return f(*args, **kwargs)

        return decorated

    @staticmethod
    def _json_response(data=None, status='success', message=None, http_status=200):
        """Helper to build consistent JSON responses."""
        resp = {'status': status}
        if message:
            resp['message'] = message
        if data is not None:
            resp['data'] = data
        return jsonify(resp), http_status

    def route_registration(self):
        """Register all routes including page routes and API routes"""
        self.app.add_url_rule('/', 'index', self.index, methods=['GET'])
        self.app.add_url_rule('/favicon.ico', 'favicon', self.favicon, methods=['GET'])
        self.app.add_url_rule('/views/<path:filename>', 'serve_views', self.serve_views, methods=['GET'])

        # Public routes (no auth required)
        self.app.add_url_rule(f'{self.route_prefix}/login', 'login', self.login, methods=['POST'])
        self.app.add_url_rule(f'{self.route_prefix}/check-auth', 'check_auth', self.check_auth, methods=['GET'])

        # Protected API routes (require login session)
        self.app.add_url_rule(f'{self.route_prefix}/logout', 'logout',
                              self._login_required(self.logout), methods=['POST'])

        self.app.add_url_rule(f'{self.route_prefix}/server/list', 'query_server_list',
                              self._login_required(self.query_server_list), methods=['GET'])
        self.app.add_url_rule(f'{self.route_prefix}/server/stats', 'server_stats',
                              self._login_required(self.server_stats), methods=['GET'])
        self.app.add_url_rule(f'{self.route_prefix}/server/max_process', 'update_max_process',
                              self._login_required(self.update_max_process), methods=['POST'])

        self.app.add_url_rule(f'{self.route_prefix}/task/list', 'query_task_list',
                              self._login_required(self.query_task_list), methods=['GET'])
        self.app.add_url_rule(f'{self.route_prefix}/task/stats', 'task_stats',
                              self._login_required(self.task_stats), methods=['GET'])
        self.app.add_url_rule(f'{self.route_prefix}/task/queues', 'task_queues',
                              self._login_required(self.task_queues), methods=['GET'])
        self.app.add_url_rule(f'{self.route_prefix}/task/retry', 'retry_task',
                              self._login_required(self.retry_task), methods=['POST'])
        self.app.add_url_rule(f'{self.route_prefix}/task/terminate', 'forced_termination_task',
                              self._login_required(self.forced_termination_task), methods=['POST'])
        self.app.add_url_rule(f'{self.route_prefix}/task/kill', 'kill_task_process',
                              self._login_required(self.kill_task_process), methods=['POST'])
        self.app.add_url_rule(f'{self.route_prefix}/task/detail/<task_id>', 'task_detail',
                              self._login_required(self.task_detail), methods=['GET'])

        self.app.register_error_handler(404, self.not_found)
        self.app.register_error_handler(500, self.server_error)

    @staticmethod
    def favicon():
        """Return simple SVG favicon"""
        return '''data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
               <rect width="100" height="100" rx="20" fill="%237c6ef0"/>
               <text x="50" y="65" font-size="50" text-anchor="middle" fill="white">⚡</text>
           </svg>''', 200, {'Content-Type': 'image/svg+xml'}

    @staticmethod
    def not_found(error):
        """Handle 404 error"""
        return jsonify({
            'status': 'error',
            'message': 'Resource not found',
            'code': 404
        }), 404

    def server_error(self, error):
        """Handle 500 error"""
        self.logger.error(f"Server error: {error}")
        return jsonify({
            'status': 'error',
            'message': 'Internal server error',
            'code': 500
        }), 500

    def serve_views(self, filename):
        """Serve static HTML files from views directory"""
        try:
            return send_from_directory(self.views_dir, filename)
        except FileNotFoundError:
            abort(404)

    @staticmethod
    def index():
        """Home page, redirect to server management or login page"""
        if session.get('logged_in'):
            return redirect('/views/server.html')
        return redirect('/views/login.html')

    @staticmethod
    def check_auth():
        """Check login status"""
        return jsonify({
            'status': 'success',
            'data': {
                'logged_in': session.get('logged_in', False),
                'username': session.get('username', '')
            }
        })

    def login(self):
        """User login with IP-based rate limiting"""
        data = request.get_json() or {}
        username = data.get('username', '').strip()
        password = data.get('password', '').strip()
        ip = request.remote_addr or 'unknown'

        if not username or not password:
            return self._json_response(
                status='error', message='Username and password cannot be empty', http_status=400
            )

        # Rate limit: max _max_login_attempts per _login_window_seconds
        now = time.time()
        self._login_attempts[ip] = [
            t for t in self._login_attempts[ip]
            if now - t < self._login_window_seconds
        ]
        if len(self._login_attempts[ip]) >= self._max_login_attempts:
            retry_after = int(self._login_window_seconds - (now - self._login_attempts[ip][0]))
            self.logger.warning(f"Login rate limit hit from {ip}")
            return jsonify({
                'status': 'error',
                'message': f'Too many login attempts. Try again in {retry_after} seconds.'
            }), 429

        if username == self.username and password == self.password:
            session['logged_in'] = True
            session['username'] = username
            session.permanent = True
            self._login_attempts[ip].clear()
            self.logger.info(f"User '{username}' logged in successfully from {ip}")
            return self._json_response(
                data={'username': username}, message='Login successful'
            )

        self._login_attempts[ip].append(now)
        self.logger.warning(f"Login failed for user '{username}' from {ip}")
        return self._json_response(
            status='error', message='Invalid username or password', http_status=401
        )

    def logout(self):
        """User logout"""
        username = session.get('username', 'unknown')
        session.clear()
        self.logger.info(f"User '{username}' logged out")
        return jsonify({
            'status': 'success',
            'message': 'Logged out successfully'
        })

    def query_server_list(self):
        """Get server list with worker information"""
        from astraflux.exports import (
            get_all_service_names, redis_scan_workers_by_service,
            redis_get_full_worker_data
        )

        try:
            service_names = get_all_service_names()
            server_list = []

            for service_name in service_names:
                worker_ids = redis_scan_workers_by_service(service_name=service_name)
                workers = []

                for unique_id in worker_ids:
                    try:
                        worker_data = redis_get_full_worker_data(unique_id=unique_id)
                        if worker_data:
                            max_process = worker_data.get('worker_max_process', 10)
                            running = worker_data.get('worker_run_process', [])
                            run_process_count = len(running) if isinstance(running, list) else 0
                            available_slots = max(0, max_process - run_process_count)

                            workers.append({
                                'unique_id': unique_id,
                                'worker_name': worker_data.get('worker_name', ''),
                                'service_ipaddr': worker_data.get('worker_ipaddr', ''),
                                'version': worker_data.get('worker_version', ''),
                                'max_process': max_process,
                                'run_process_count': run_process_count,
                                'available_slots': available_slots,
                                'run_process': running,
                                'functions': worker_data.get('worker_functions', {}),
                                'pid': worker_data.get('worker_pid')
                            })
                    except Exception as e:
                        self.logger.warning(f"Failed to get worker data for {unique_id}: {e}")
                        continue

                server_list.append({
                    'service_name': service_name,
                    'worker_count': len(workers),
                    'workers': workers,
                    'total_slots': sum(w.get('available_slots', 0) for w in workers),
                    'total_running': sum(w.get('run_process_count', 0) for w in workers)
                })

            return jsonify({
                'status': 'success',
                'data': server_list
            })
        except Exception as e:
            self.logger.error(f"Query server list failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def server_stats(self):
        """Get server statistics"""
        from astraflux.exports import get_all_service_names, redis_scan_workers_by_service, redis_get_worker_status

        try:
            service_names = get_all_service_names()
            stats = {
                'total_services': len(service_names),
                'total_workers': 0,
                'total_max_process': 0,
                'total_running': 0,
                'total_available': 0
            }

            for service_name in service_names:
                worker_ids = redis_scan_workers_by_service(service_name=service_name)
                stats['total_workers'] += len(worker_ids)

                for unique_id in worker_ids:
                    status = redis_get_worker_status(unique_id=unique_id)
                    if status:
                        stats['total_max_process'] += status.get('max_process', 0)
                        stats['total_running'] += status.get('run_process_count', 0)
                        stats['total_available'] += status.get('available_slots', 0)

            return jsonify({
                'status': 'success',
                'data': stats
            })
        except Exception as e:
            self.logger.error(f"Server stats failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def update_max_process(self):
        """Update worker max process count"""
        from astraflux.exports import redis_update_max_process

        data = request.get_json() or {}
        unique_id = data.get('unique_id')
        max_process = data.get('max_process')

        if not unique_id:
            return jsonify({
                'status': 'error',
                'message': 'Missing parameter: unique_id'
            }), 400

        if max_process is None:
            return jsonify({
                'status': 'error',
                'message': 'Missing parameter: max_process'
            }), 400

        try:
            max_process = int(max_process)
            if max_process < 1:
                raise ValueError("Max process must be >= 1")
            if max_process > 100:
                raise ValueError("Max process cannot exceed 100")

            redis_update_max_process(unique_id=unique_id, new_value=max_process)
            self.logger.info(f"Updated max_process for {unique_id} to {max_process}")

            return jsonify({
                'status': 'success',
                'message': 'Update successful',
                'data': {
                    'unique_id': unique_id,
                    'max_process': max_process
                }
            })
        except ValueError as e:
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 400
        except Exception as e:
            self.logger.error(f"Update max_process failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def query_task_list(self):
        """Get paginated task list"""
        from astraflux.exports import mongodb_find_paginated_from_task

        status = request.args.get('status', '').strip().lower()
        queue_name = request.args.get('queue_name', '').strip()
        task_id = request.args.get('task_id', '').strip()
        date = request.args.get('date', '').strip()

        try:
            page = max(1, int(request.args.get('page', 1)))
            page_size = min(100, max(10, int(request.args.get('page_size', 20))))
        except ValueError:
            page, page_size = 1, 20

        query = {}
        if status:
            query[TASK.CONFIG.STATUS.value] = status
        if queue_name:
            query[TASK.CONFIG.QUEUE_NAME.value] = queue_name
        if task_id:
            query[TASK.CONFIG.ID.value] = {'$regex': task_id, '$options': 'i'}
        if date:
            query[TASK.CONFIG.CREATE_TIME.value] = {'$regex': f'^{date}'}

        try:
            total, tasks = mongodb_find_paginated_from_task(
                query=query,
                fields={'_id': 0},
                limit=page_size,
                skip=(page - 1) * page_size,
                sort_field=TASK.CONFIG.CREATE_TIME.value,
                sort_order=-1
            )

            return jsonify({
                'status': 'success',
                'data': {
                    'total': total,
                    'page': page,
                    'page_size': page_size,
                    'tasks': tasks
                }
            })
        except Exception as e:
            self.logger.error(f"Query task list failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def task_stats(self):
        """Get task statistics"""
        from astraflux.exports import mongodb_find_from_task

        try:
            stats = {}

            status_list = [
                STATUS.PENDING.value,
                STATUS.WAITING.value,
                STATUS.RUNNING.value,
                STATUS.SUCCESS.value,
                STATUS.FAILED.value,
                STATUS.STOPPED.value,
                STATUS.RETRYING.value
            ]

            for s in status_list:
                tasks = mongodb_find_from_task(
                    query={TASK.CONFIG.STATUS.value: s},
                    fields={'_id': 0, TASK.CONFIG.ID.value: 1}
                )
                stats[s.upper()] = len(tasks) if tasks else 0

            stats['TOTAL'] = sum(stats.values())

            return jsonify({
                'status': 'success',
                'data': stats
            })
        except Exception as e:
            self.logger.error(f"Task stats failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def task_queues(self):
        """Get all queue names"""
        from astraflux.exports import mongodb_find_from_task

        try:
            tasks = mongodb_find_from_task(
                query={},
                fields={'_id': 0, TASK.CONFIG.QUEUE_NAME.value: 1}
            )

            queues = list(
                set(t.get(TASK.CONFIG.QUEUE_NAME.value, '') for t in tasks if t.get(TASK.CONFIG.QUEUE_NAME.value)))
            queues.sort()

            return jsonify({
                'status': 'success',
                'data': queues
            })
        except Exception as e:
            self.logger.error(f"Task queues failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def task_detail(self, task_id):
        """Get task details"""
        from astraflux.exports import mongodb_find_from_task

        try:
            tasks = mongodb_find_from_task(
                query={TASK.CONFIG.ID.value: task_id},
                fields={'_id': 0}
            )

            if not tasks:
                return jsonify({
                    'status': 'error',
                    'message': 'Task not found'
                }), 404

            return jsonify({
                'status': 'success',
                'data': tasks[0]
            })
        except Exception as e:
            self.logger.error(f"Task detail failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def retry_task(self):
        """Retry task - reset task status to PENDING"""
        from astraflux.exports import mongodb_find_one_and_update_from_task, mongodb_find_from_task

        data = request.get_json() or {}
        task_id = data.get('task_id')

        if not task_id:
            return jsonify({
                'status': 'error',
                'message': 'Missing parameter: task_id'
            }), 400

        try:
            tasks = mongodb_find_from_task(
                query={TASK.CONFIG.ID.value: task_id},
                fields={'_id': 0, TASK.CONFIG.STATUS.value: 1}
            )

            if not tasks:
                return jsonify({
                    'status': 'error',
                    'message': 'Task not found'
                }), 404

            mongodb_find_one_and_update_from_task(
                query={TASK.CONFIG.ID.value: task_id},
                data={
                    TASK.CONFIG.STATUS.value: STATUS.PENDING.value,
                    TASK.CONFIG.START_TIME.value: None,
                    TASK.CONFIG.END_TIME.value: None,
                    TASK.CONFIG.ERROR_MESSAGE.value: None
                },
                upsert=False
            )

            self.logger.info(f"Task {task_id} retry triggered")
            return jsonify({
                'status': 'success',
                'message': 'Task added to retry queue',
                'data': {'task_id': task_id}
            })
        except Exception as e:
            self.logger.error(f"Retry task failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def forced_termination_task(self):
        """Force terminate task - update status and attempt to kill process"""
        from astraflux.exports import mongodb_find_one_and_update_from_task, mongodb_find_from_task
        from astraflux.core import global_manager

        data = request.get_json() or {}
        task_id = data.get('task_id')

        if not task_id:
            return jsonify({
                'status': 'error',
                'message': 'Missing parameter: task_id'
            }), 400

        try:
            tasks = mongodb_find_from_task(
                query={TASK.CONFIG.ID.value: task_id},
                fields={'_id': 0}
            )

            if not tasks:
                return jsonify({
                    'status': 'error',
                    'message': 'Task not found'
                }), 404

            current_status = tasks[0].get(TASK.CONFIG.STATUS.value, '')
            worker_pid = tasks[0].get(BUILD.CONFIG.WORKER_PID.value)
            worker_unique_id = tasks[0].get(BUILD.CONFIG.UNIQUE_ID.value)

            mongodb_find_one_and_update_from_task(
                query={TASK.CONFIG.ID.value: task_id},
                data={
                    TASK.CONFIG.STATUS.value: STATUS.STOPPED.value,
                    TASK.CONFIG.END_TIME.value: datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                },
                upsert=False
            )

            process_killed = False
            if worker_pid and current_status == STATUS.RUNNING.value:
                try:
                    if os.name == 'nt':
                        import subprocess
                        subprocess.run(
                            ['taskkill', '/F', '/PID', str(worker_pid)],
                            capture_output=True, timeout=5, check=False
                        )
                    else:
                        import signal
                        os.kill(worker_pid, signal.SIGTERM)
                    process_killed = True
                    self.logger.info(f"Terminated process {worker_pid} for task {task_id}")
                except ProcessLookupError:
                    process_killed = True
                    self.logger.info(f"Process {worker_pid} already exited for task {task_id}")
                except Exception as e:
                    self.logger.warning(f"Failed to kill process {worker_pid}: {e}")

            if worker_unique_id and worker_pid:
                try:
                    from astraflux.exports import redis_remove_from_run_process
                    redis_remove_from_run_process(
                        unique_id=worker_unique_id,
                        process_id=worker_pid
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to remove from run_process: {e}")

            try:
                redis_client = global_manager.get_fixture('fixture_redis_client')
                conn = redis_client.get_connection()
                conn.delete(f"task_process:{task_id}")
            except Exception as e:
                self.logger.warning(f"Failed to clean task_process mapping: {e}")

            self.logger.info(f"Task {task_id} terminated (was: {current_status}, process_killed: {process_killed})")
            return jsonify({
                'status': 'success',
                'message': 'Task terminated' + (' with kill signal sent' if process_killed else ''),
                'data': {
                    'task_id': task_id,
                    'previous_status': current_status,
                    'process_killed': process_killed
                }
            })
        except Exception as e:
            self.logger.error(f"Forced termination task failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def kill_task_process(self):
        """Kill task process directly via task_process mapping in Redis"""
        import os
        from astraflux.core import global_manager
        from astraflux.exports import mongodb_find_one_and_update_from_task

        data = request.get_json() or {}
        task_id = data.get('task_id')

        if not task_id:
            return jsonify({
                'status': 'error',
                'message': 'Missing parameter: task_id'
            }), 400

        try:
            redis_client = global_manager.get_fixture('fixture_redis_client')
            conn = redis_client.get_connection()
            key = f"task_process:{task_id}"
            process_data = conn.hgetall(key)

            if not process_data:
                return jsonify({
                    'status': 'error',
                    'message': 'Task process mapping not found, task may not be running or already finished'
                }), 404

            process_id = int(process_data[b'process_id'].decode())
            unique_id = process_data[b'unique_id'].decode()

            try:
                if os.name == 'nt':
                    import subprocess
                    subprocess.run(
                        ['taskkill', '/F', '/PID', str(process_id)],
                        capture_output=True, timeout=5, check=False
                    )
                else:
                    import signal
                    os.kill(process_id, signal.SIGTERM)
                self.logger.info(f"Killed process {process_id} for task {task_id}")
            except ProcessLookupError:
                self.logger.info(f"Process {process_id} already exited")
            except PermissionError:
                return jsonify({
                    'status': 'error',
                    'message': f'Permission denied to terminate process {process_id}'
                }), 403

            conn.delete(key)

            try:
                from astraflux.exports import redis_remove_from_run_process
                redis_remove_from_run_process(unique_id=unique_id, process_id=process_id)
            except Exception as e:
                return jsonify({
                    'status': 'error',
                    'message': str(e)
                }), 500

            mongodb_find_one_and_update_from_task(
                query={TASK.CONFIG.ID.value: task_id},
                data={
                    TASK.CONFIG.STATUS.value: STATUS.STOPPED.value,
                    TASK.CONFIG.END_TIME.value: datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                },
                upsert=False
            )

            return jsonify({
                'status': 'success',
                'message': f'Process {process_id} terminated',
                'data': {
                    'task_id': task_id,
                    'process_id': process_id
                }
            })

        except Exception as e:
            self.logger.error(f"Kill task process failed: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500

    def web_launch(self):
        """Launch the web application"""
        self.route_registration()
        self.logger.info(f"Web application starting on {self.bind_ip}:{self.port}")
        self.app.run(
            host=self.bind_ip,
            port=self.port,
            debug=False,
            threaded=True
        )


def main():
    """Command line entry point"""
    parser = argparse.ArgumentParser(description="AstraFlux Web Application Launcher")

    parser.add_argument("--yaml_file", type=str, required=True,
                        help="Path to YAML configuration file")
    parser.add_argument("--class_path", type=str, required=True,
                        help="Path to service class definition file")
    parser.add_argument("--current_dir", type=str, required=True,
                        help="Current working directory")

    args = parser.parse_args()

    sys.path.insert(0, args.current_dir)

    AstraFlux(yaml_path=args.yaml_file, current_dir=args.current_dir)

    _config = config_obj().get(WEB.CONFIG.KEY.value)

    WebApp(config=_config).web_launch()


if __name__ == '__main__':
    main()
