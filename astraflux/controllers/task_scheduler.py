# -*- encoding: utf-8 -*-

import copy
import threading
from typing import List, Dict
from collections import defaultdict, deque

from astraflux.config.constants import *
from astraflux.exports.logger import logger
from astraflux.exports.mq import rabbitmq_send_message
from astraflux.exports.redisdb import get_total_available_slots_by_server_name
from astraflux.exports.mongodb import mongodb_find_paginated_from_task, mongodb_find_one_and_update_from_task


class TaskScheduler:
    """
    Task Scheduler responsible for managing and dispatching tasks based on dependencies,
    priorities, and available worker resources.

    The scheduler handles:
    - Fetching pending/running/retrying tasks from MongoDB
    - Building dependency graphs between tasks
    - Propagating failures through the dependency chain
    - Identifying runnable tasks based on dependency satisfaction
    - Prioritizing subtasks of running parent tasks
    - Dispatching tasks to appropriate message queues based on worker availability
    - Updating task statuses and persisting changes back to database
    """

    # Class-level lock to prevent concurrent scheduling cycles
    _execute_lock = threading.Lock()

    def __init__(self):
        """Initialize the TaskScheduler with a logger and final status set."""
        self.logger = logger(dirname=PROJECT.NAME.value, filename='task_scheduler')

        # Define terminal states for tasks (tasks that have completed execution)
        self.FINAL_STATUS = {STATUS.SUCCESS.value, STATUS.FAILED.value, STATUS.STOPPED.value}

    def execute(self):
        """
        Main execution method that runs a complete scheduling cycle.

        Uses a non-blocking class-level lock to prevent overlapping scheduling cycles.
        If a previous cycle is still running, the current cycle is skipped to avoid
        data races and duplicate task dispatching.

        The scheduling cycle consists of:
        1. Fetching active tasks from database
        2. Building dependency relationships between tasks
        3. Propagating failures through the graph
        4. Finding tasks that are ready to run
        5. Identifying priority subtasks
        6. Dispatching tasks to queues based on worker availability
        7. Updating parent task statuses based on children
        8. Persisting all status changes back to database
        """
        # Prevent re-entrant scheduling cycles (the cron runs every 10s,
        # but processing 1000+ tasks can exceed that)
        if not self._execute_lock.acquire(blocking=False):
            self.logger.warning("Previous scheduling cycle still running, skipping this one")
            return

        try:
            self.logger.debug("Starting task scheduling cycle")

            # Step 1: Fetch active tasks from database
            tasks = self._fetch_tasks()
            if not tasks:
                self.logger.debug("No tasks to process")
                return

            # Step 2: Build dependency graph structure
            tasks_map, graph, parent_children = self._build_dependency_graph(tasks)

            # Step 3: Propagate failures through the graph
            self._propagate_failures(tasks_map, graph, parent_children)

            # Step 4: Find tasks that are ready to execute
            runnable_tasks = self._find_runnable_tasks(tasks_map, graph)

            # Step 5: Identify priority subtasks (children of running parents)
            priority_subtasks = self._get_priority_subtasks(tasks_map, parent_children)

            # Step 6: Schedule tasks to queues based on priority and worker availability
            self._schedule(tasks_map, runnable_tasks, priority_subtasks)

            # Step 7: Update parent task statuses based on child task completion
            self._update_parent_status(tasks_map, parent_children)

            # Step 8: Persist all status updates to database
            self._persist_updates(tasks_map)

            self.logger.debug("Scheduling cycle completed")
        finally:
            self._execute_lock.release()

    @staticmethod
    def _fetch_tasks(limit=DEFAULTS.SCHEDULER_TASK_FETCH_LIMIT) -> List[Dict]:
        """
        Fetch active tasks from MongoDB that need scheduling.

        Args:
            limit: Maximum number of tasks to fetch (default: 1000)

        Returns:
            List of task documents from database
        """
        # Query for tasks that are not in terminal states
        query = {
            TASK.CONFIG.STATUS.value: {
                '$in': [STATUS.PENDING.value, STATUS.RUNNING.value, STATUS.RETRYING.value, STATUS.WAITING.value]
            }
        }
        count, data = mongodb_find_paginated_from_task(
            query=query,
            fields={'_id': 0},  # Exclude MongoDB internal ID
            limit=limit
        )
        return data

    def _build_dependency_graph(self, tasks: List[Dict]) -> tuple:
        """
        Build a directed graph representing task dependencies.

        Creates three data structures:
        - tasks_map: Quick lookup of tasks by ID
        - graph: Adjacency lists for both incoming and outgoing dependencies
        - parent_children: Mapping of parent tasks to their subtasks

        Args:
            tasks: List of task documents

        Returns:
            Tuple containing (tasks_map, graph, parent_children)
        """
        tasks_map = {}  # Maps task_id -> task document
        graph = {'out': defaultdict(set), 'in': defaultdict(set)}  # out: dependencies, in: dependents
        parent_children = defaultdict(list)  # Maps parent_id -> list of child task IDs

        for task in tasks:
            task_id = task[TASK.CONFIG.ID.value]
            # Snapshot original status for change detection in _persist_updates
            task['_original_status'] = task.get(TASK.CONFIG.STATUS.value)
            tasks_map[task_id] = task

            # Track parent-child relationships
            source_id = task.get(TASK.CONFIG.SOURCE_ID.value)
            if source_id:
                parent_children[source_id].append(task_id)

            # Track explicit dependencies
            depends = task.get(TASK.CONFIG.DEPENDS_ON.value)
            if depends and isinstance(depends, list):
                for dep_id in depends:
                    if dep_id:
                        graph['in'][task_id].add(dep_id)  # task_id depends on dep_id
                        graph['out'][dep_id].add(task_id)  # dep_id is prerequisite for task_id

        self.logger.debug(f"Built graph: {len(tasks_map)} tasks, {len(graph['in'])} in-edges")
        return tasks_map, graph, parent_children

    def _propagate_failures(self, tasks_map: Dict, graph: Dict, parent_children: Dict):
        """
        Propagate failure status through the dependency graph.

        When a task fails, all tasks that depend on it should also fail.
        Additionally, if a subtask fails, its parent task should fail.

        Args:
            tasks_map: Mapping of task IDs to task documents
            graph: Dependency graph structure
            parent_children: Mapping of parent tasks to their children
        """
        # Initialize queue with all failed tasks
        queue = deque()
        for task_id, task in tasks_map.items():
            if task[TASK.CONFIG.STATUS.value] == STATUS.FAILED.value:
                queue.append(task_id)

        # BFS traversal to propagate failures
        while queue:
            failed_id = queue.popleft()

            # Mark all dependents as failed
            for dependent_id in graph['out'].get(failed_id, []):
                if dependent_id in tasks_map:
                    dep_task = tasks_map[dependent_id]
                    if dep_task[TASK.CONFIG.STATUS.value] not in self.FINAL_STATUS:
                        self.logger.info(f"Task {dependent_id} fails due to dependency {failed_id}")
                        dep_task[TASK.CONFIG.STATUS.value] = STATUS.FAILED.value
                        queue.append(dependent_id)

            # Mark parent tasks as failed if a child failed
            for parent_id, children in parent_children.items():
                if failed_id in children and parent_id in tasks_map:
                    parent = tasks_map[parent_id]
                    if parent[TASK.CONFIG.STATUS.value] not in self.FINAL_STATUS:
                        self.logger.info(f"Parent task {parent_id} fails due to child {failed_id}")
                        parent[TASK.CONFIG.STATUS.value] = STATUS.FAILED.value
                        queue.append(parent_id)

        self.logger.debug("Failure propagation completed")

    def _find_runnable_tasks(self, tasks_map: Dict, graph: Dict) -> List[str]:
        """
        Identify tasks that are ready to run based on their dependencies.

        A task is runnable if:
        - It's in PENDING or RETRYING state
        - All its dependencies have completed successfully

        Args:
            tasks_map: Mapping of task IDs to task documents
            graph: Dependency graph structure

        Returns:
            List of task IDs that are ready to run
        """
        runnable = []
        for task_id, task in tasks_map.items():
            status = task[TASK.CONFIG.STATUS.value]
            # Skip tasks that aren't ready to run
            if status not in (STATUS.PENDING.value, STATUS.RETRYING.value):
                continue

            # Check if all dependencies are satisfied
            deps = graph['in'].get(task_id, set())
            deps_satisfied = True
            for dep_id in deps:
                dep_task = tasks_map.get(dep_id)
                if not dep_task or dep_task[TASK.CONFIG.STATUS.value] != STATUS.SUCCESS.value:
                    deps_satisfied = False
                    break

            if deps_satisfied:
                runnable.append(task_id)

        self.logger.debug(f"Found {len(runnable)} runnable tasks")
        return runnable

    def _get_priority_subtasks(self, tasks_map: Dict, parent_children: Dict) -> List[str]:
        """
        Identify subtasks that should be prioritized for execution.

        Subtasks of currently RUNNING parent tasks are considered priority
        to enable faster completion of the parent workflow.

        Args:
            tasks_map: Mapping of task IDs to task documents
            parent_children: Mapping of parent tasks to their children

        Returns:
            List of priority subtask IDs
        """
        priority = []
        for parent_id, children in parent_children.items():
            parent = tasks_map.get(parent_id)
            if parent and parent[TASK.CONFIG.STATUS.value] == STATUS.RUNNING.value:
                priority.extend(children)
        self.logger.debug(f"Found {len(priority)} priority subtasks (parents are RUNNING)")
        return priority

    @staticmethod
    def _fetch_idle_workers(server_name) -> int:
        """
        Get the number of idle workers available for a specific queue/server.

        Args:
            server_name: Name of the queue/server to check

        Returns:
            Number of available worker slots
        """
        return get_total_available_slots_by_server_name(server_name=server_name)

    def _schedule(self, tasks_map, runnable_tasks: List[str], priority_subtasks: List[str]):
        """
        Dispatch runnable tasks to appropriate queues based on priority and worker availability.

        Tasks are organized by queue, with priority tasks (subtasks of running parents)
        being scheduled before normal tasks. Within each priority level, tasks are sorted
        by weight (higher weight first).

        Args:
            tasks_map: Mapping of task IDs to task documents
            runnable_tasks: List of task IDs that are ready to run
            priority_subtasks: List of task IDs that should be prioritized
        """
        # Organize tasks by queue and priority
        queue_tasks = defaultdict(lambda: {'priority': [], 'normal': []})

        for task_id in runnable_tasks:
            task = tasks_map[task_id]
            queue = task[TASK.CONFIG.QUEUE_NAME.value]
            if task_id in priority_subtasks:
                queue_tasks[queue]['priority'].append(task_id)
            else:
                queue_tasks[queue]['normal'].append(task_id)

        # Sort tasks within each queue by weight (descending)
        for queue in queue_tasks:
            queue_tasks[queue]['priority'].sort(
                key=lambda tid: tasks_map[tid][TASK.CONFIG.WEIGHT.value], reverse=True)
            queue_tasks[queue]['normal'].sort(
                key=lambda tid: tasks_map[tid][TASK.CONFIG.WEIGHT.value], reverse=True)

        # Schedule tasks respecting worker capacity
        for queue, tasks_by_type in queue_tasks.items():
            idle_workers_number = self._fetch_idle_workers(server_name=queue)
            idle = copy.deepcopy(idle_workers_number)
            if idle_workers_number <= 0:
                self.logger.info(f"Queue {queue} has no idle workers, skip")
                continue

            # Schedule priority tasks first
            for task_id in tasks_by_type['priority']:
                if idle <= 0:
                    break
                self._send_task(tasks_map=tasks_map, task_id=task_id, queue=queue)
                idle -= 1

            # Then schedule normal tasks
            for task_id in tasks_by_type['normal']:
                if idle <= 0:
                    break
                self._send_task(tasks_map=tasks_map, task_id=task_id, queue=queue)
                idle -= 1

            self.logger.debug(f"Scheduled {idle_workers_number - idle} tasks to queue {queue}")

    def _send_task(self, tasks_map, task_id: str, queue: str):
        """
        Send a task to the message queue for execution.

        Args:
            tasks_map: Mapping of task IDs to task documents
            task_id: ID of the task to send
            queue: Name of the queue to send the task to
        """
        task = tasks_map[task_id]
        try:
            # Publish task to RabbitMQ
            rabbitmq_send_message(queue=queue, message=task[TASK.CONFIG.BODY.value])
            self.logger.debug(f"Sent task {task_id} to queue {queue}")
            # Mark task as waiting for worker pickup
            task[TASK.CONFIG.STATUS.value] = STATUS.WAITING.value
        except Exception as e:
            self.logger.error(f"Failed to send task {task_id} to queue {queue}: {e}")

    def _update_parent_status(self, tasks_map: Dict, parent_children: Dict):
        """
        Update parent task statuses based on the status of their children.

        Rules:
        - If all children succeeded -> parent succeeds
        - If any child failed -> parent fails
        - Otherwise, parent status remains unchanged

        Args:
            tasks_map: Mapping of task IDs to task documents
            parent_children: Mapping of parent tasks to their children
        """
        for parent_id, children in parent_children.items():
            parent = tasks_map.get(parent_id)
            if not parent or parent[TASK.CONFIG.STATUS.value] in self.FINAL_STATUS:
                continue

            # Collect statuses of existing child tasks
            child_statuses = [tasks_map[cid][TASK.CONFIG.STATUS.value] for cid in children if cid in tasks_map]
            if not child_statuses:
                continue

            # Determine parent status based on children
            all_success = all(s == STATUS.SUCCESS.value for s in child_statuses)
            any_failed = any(s == STATUS.FAILED.value for s in child_statuses)

            if all_success:
                self.logger.debug(f"Parent task {parent_id} all children succeeded, marking SUCCESS")
                parent[TASK.CONFIG.STATUS.value] = STATUS.SUCCESS.value
            elif any_failed:
                self.logger.debug(f"Parent task {parent_id} has failed child, marking FAILED")
                parent[TASK.CONFIG.STATUS.value] = STATUS.FAILED.value

    def _persist_updates(self, tasks_map: Dict):
        """
        Persist all task status changes back to MongoDB.

        Handles two types of updates:
        1. PENDING/RETRYING -> WAITING: Tasks that were dispatched to workers
        2. Parent status changes: SUCCESS/FAILED propagation from children

        Uses individual conditional updates to ensure only the correct state
        transitions are applied, preventing concurrent worker writes from
        being overwritten.

        Args:
            tasks_map: Mapping of task IDs to updated task documents.
                       Tasks carry '_original_status' set in _build_dependency_graph.
        """
        for task_id, task in tasks_map.items():
            new_status = task[TASK.CONFIG.STATUS.value]
            old_status = task.get('_original_status')

            if new_status == STATUS.WAITING.value and old_status in [STATUS.PENDING.value, STATUS.RETRYING.value]:
                # Task dispatched to worker: apply conditional update
                mongodb_find_one_and_update_from_task(
                    query={
                        TASK.CONFIG.ID.value: task_id,
                        TASK.CONFIG.STATUS.value: old_status
                    },
                    data={TASK.CONFIG.STATUS.value: STATUS.WAITING.value},
                    upsert=False
                )
                self.logger.debug(f"Update Task Status for {task_id} to Waiting from {old_status}")

            elif old_status != new_status and new_status in self.FINAL_STATUS:
                # Parent status changed (SUCCESS/FAILED) via _update_parent_status: persist it
                mongodb_find_one_and_update_from_task(
                    query={TASK.CONFIG.ID.value: task_id},
                    data={TASK.CONFIG.STATUS.value: new_status},
                    upsert=False
                )
                self.logger.debug(f"Update parent task {task_id} status to {new_status}")
