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
Module contains multithread_executor.

:author: M. Marin (KTH)
:date:   1.11.2020
"""

from dagster.core.definitions.executor import Int, Field, Retries, check, executor, get_retries_config


@executor(
    name="multithread",
    config_schema={"max_concurrent": Field(Int, is_required=False, default_value=0), "retries": get_retries_config()},
)
def multithread_executor(init_context):
    """A custom multithread executor.

    This simple multithread executor borrows almost all the code from dagster's builtin multiprocess executor,
    but takes a twist to use threading instead of multiprocessing.
    To select the multithread executor, include a fragment
    such as the following in your config:

    .. code-block:: yaml

        execution:
          multithread:
            config:
                max_concurrent: 4

    The ``max_concurrent`` arg is optional and tells the execution engine how many threads may run
    concurrently. By default, or if you set ``max_concurrent`` to be 0, this is 100
    (in attendance of a better method).

    Execution priority can be configured using the ``dagster/priority`` tag via solid metadata,
    where the higher the number the higher the priority. 0 is the default and both positive
    and negative numbers can be used.
    """
    from dagster.core.executor.init import InitExecutorContext
    from spine_engine.multithread_executor.multithread import MultithreadExecutor

    check.inst_param(init_context, "init_context", InitExecutorContext)

    return MultithreadExecutor(
        max_concurrent=init_context.executor_config["max_concurrent"],
        retries=Retries.from_config(init_context.executor_config["retries"]),
    )
