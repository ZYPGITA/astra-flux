# -*- coding: utf-8 -*-

from astraflux.core import global_manager
from astraflux.config.constants import DEFAULTS


def mongodb_find_one_and_update_from_task(query: dict, data: dict, upsert: bool = True):
    """
    Finds a single document in the 'TaskCollection' collection and updates it, or inserts a new one if not found.

    Args:
        query (dict): A dictionary specifying the filter to find the document.
        data (dict): A dictionary containing the fields and values to update using the `$set` operator.
        upsert (bool, optional): If True, creates a new document when no match is found. Defaults to True.

    Returns:
        pymongo.results.UpdateResult: An object that contains information about the update operation.

    Note:
        This function uses the `global_manager` to retrieve the `fixture_mongodb_tasks` client instance.
    """

    def _backcall(fixture_mongodb_tasks):
        return fixture_mongodb_tasks.find_one_and_update(query=query, data=data, upsert=upsert)

    return global_manager.bind_fixture_func(_backcall)()


def mongodb_array_push_from_task(query: dict, data: dict):
    """
    Appends a value to an array field in a document within the 'TaskCollection' collection.

    Args:
        query (dict): A dictionary specifying the filter to find the document.
        data (dict): A dictionary where the key is the array field name and the value is the item to append,
            used with the `$push` operator.

    Returns:
        pymongo.results.UpdateResult: An object that contains information about the update operation.

    Note:
        This function uses the `global_manager` to retrieve the `fixture_mongodb_tasks` client instance.
    """

    def _backcall(fixture_mongodb_tasks):
        return fixture_mongodb_tasks.array_push(query=query, data=data)

    return global_manager.bind_fixture_func(_backcall)()


def mongodb_array_pull_from_task(query: dict, data: dict):
    """
    Removes a value from an array field in a document within the 'TaskCollection' collection.

    Args:
        query (dict): A dictionary specifying the filter to find the document.
        data (dict): A dictionary where the key is the array field name and the value is the item to remove,
            used with the `$pull` operator.

    Returns:
        pymongo.results.UpdateResult: An object that contains information about the update operation.

    Note:
        This function uses the `global_manager` to retrieve the `fixture_mongodb_tasks` client instance.
    """

    def _backcall(fixture_mongodb_tasks):
        return fixture_mongodb_tasks.array_pull(query=query, data=data)

    return global_manager.bind_fixture_func(_backcall)()


def mongodb_insert_from_task(data: dict):
    """
    Inserts a single document into the 'TaskCollection' collection.

    Args:
        data (dict): A dictionary representing the document to be inserted.

    Returns:
        pymongo.results.InsertOneResult: An object that contains information about the insert operation,
            including the inserted document's _id.

    Note:
        This function uses the `global_manager` to retrieve the `fixture_mongodb_tasks` client instance.
    """

    def _backcall(fixture_mongodb_tasks):
        return fixture_mongodb_tasks.insert(data=data)

    return global_manager.bind_fixture_func(_backcall)()


def mongodb_delete_from_task(query: dict):
    """
    Deletes all documents matching the given query filter from the 'TaskCollection' collection.

    Args:
        query (dict): A dictionary specifying the filter to select documents for deletion.

    Returns:
        pymongo.results.DeleteResult: An object that contains information about the delete operation,
            including the number of documents deleted.

    Note:
        This function uses the `global_manager` to retrieve the `fixture_mongodb_tasks` client instance.
    """

    def _backcall(fixture_mongodb_tasks):
        return fixture_mongodb_tasks.delete(query=query)

    return global_manager.bind_fixture_func(_backcall)()


def mongodb_find_from_task(query: dict, fields: dict):
    """
    Retrieves all documents matching the provided query filter from the 'TaskCollection' collection.

    This method can optionally project specific fields to include or exclude in the result.

    Args:
        query (dict): A dictionary specifying the query filter. Use an empty dict `{}` to retrieve all documents.
        fields (dict): A dictionary specifying the field projection. Use 1 to include a field and 0 to exclude it.
            Mixing include and exclude is not allowed except for the `_id` field.

    Returns:
        List[dict]: A list of dictionaries, each representing a document that matches the query.
            Returns an empty list if no matches are found.

    Note:
        This function uses the `global_manager` to retrieve the `fixture_mongodb_tasks` client instance.
    """

    def _backcall(fixture_mongodb_tasks):
        return fixture_mongodb_tasks.find(query=query, fields=fields)

    return global_manager.bind_fixture_func(_backcall)()


def mongodb_find_paginated_from_task(
        query: dict,
        fields: dict,
        limit: int = DEFAULTS.MONGODB_PAGINATION_DEFAULT_LIMIT,
        skip: int = DEFAULTS.MONGODB_PAGINATION_DEFAULT_SKIP,
        sort_field: str = 'create_time', sort_order: int = -1):
    """
    Retrieves a paginated and sorted subset of documents matching the query from the 'TaskCollection' collection.

    This method is ideal for implementing pagination in applications, as it returns both the data for the
    current page and the total number of matching documents.

    Args:
        query (dict): A dictionary specifying the query filter.
        fields (dict): A dictionary specifying the field projection.
        limit (int, optional): The maximum number of documents to return per page. Defaults to 10.
        skip (int, optional): The number of documents to skip before starting to return results.
            Used to navigate to subsequent pages. Defaults to 0 (first page).
        sort_field (str, optional): The name of the field by which to sort the results. Defaults to 'create_time'.
        sort_order (int, optional): The sort direction. Use 1 for ascending order and -1 for descending order.
            Defaults to -1 (descending).

    Returns:
        Tuple[int, List[dict]]: A tuple containing:
            - int: The total number of documents that match the query (for pagination controls).
            - List[dict]: A list of documents for the current page.

    Note:
        This function uses the `global_manager` to retrieve the `fixture_mongodb_tasks` client instance.
        The internal call to `fixture_mongodb_tasks.find` appears to be a typo and should likely be
        `fixture_mongodb_tasks.find_paginated` to match the function's purpose and parameters.
    """

    def _backcall(fixture_mongodb_tasks):
        return fixture_mongodb_tasks.find_paginated(
            query=query, fields=fields, limit=limit, skip=skip, sort_field=sort_field, sort_order=sort_order)

    return global_manager.bind_fixture_func(_backcall)()
