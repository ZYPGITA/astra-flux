# -*- coding: utf-8 -*-

from astraflux.core import global_manager


def snowflake_id():
    """
    Generate snowflake ID

    This function is used to generate a snowflake ID by binding a fixture function
    and invoking the snowflake_id method of the fixture generator.
    """

    def _backcall(fixture_generate_id):
        """Callback function to call snowflake_id method of fixture generator

        Args:
            fixture_generate_id: Fixture generator object with snowflake_id method

        Returns:
            Any: Result of snowflake_id method call (the generated snowflake ID)
        """
        # Call the snowflake_id method of the fixture generator
        return fixture_generate_id.snowflake_id()

    # Bind the callback function to global manager's fixture function and execute it
    return global_manager.bind_fixture_func(_backcall)()
