######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Engine contributors
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
from queue import Queue
from spine_engine.utils.queue_logger import QueueLogger


class MessageStoringSlot:
    def __init__(self):
        self.message = None

    def __call__(self, message):
        self.message = message


class TestQueueLogger:
    def test_msg(self):
        queue = Queue()
        logger = QueueLogger(queue, "item's name", None, None)
        slot = MessageStoringSlot()
        logger.msg.connect(slot)
        logger.msg.emit("my message")
        assert slot.message == {"filter_id": "", "msg_type": "msg", "msg_text": "my message"}
        assert queue.get() == (
            "event_msg",
            {"filter_id": "", "item_name": "item's name", "msg_type": "msg", "msg_text": "my message"},
        )

    def test_msg_standard_execution(self):
        queue = Queue()
        logger = QueueLogger(queue, "item's name", None, None)
        slot = MessageStoringSlot()
        logger.msg_standard_execution.connect(slot)
        logger.msg_standard_execution.emit({"exec_key": "exec value"})
        assert slot.message == {"filter_id": "", "exec_key": "exec value"}
        assert queue.get() == (
            "standard_execution_msg",
            {"filter_id": "", "item_name": "item's name", "exec_key": "exec value"},
        )

    def test_set_filter_id(self):
        queue = Queue()
        logger = QueueLogger(queue, "item's name", None, None)
        logger.set_filter_id("my filter id")
        logger.msg.emit("my message")
        assert queue.get() == (
            "event_msg",
            {"filter_id": "my filter id", "item_name": "item's name", "msg_type": "msg", "msg_text": "my message"},
        )

    def test_prompt(self):
        queue = Queue()
        prompt_queue = Queue()
        answered_prompts = {}
        logger = QueueLogger(queue, "item's name", prompt_queue, answered_prompts)
        logger.set_filter_id("my filter id")
        prompt_queue.put("Here's my answer.")
        assert logger.prompt.emit("my message") == "Here's my answer."
        assert queue.get() == ("prompt", {"prompter_id": id(prompt_queue), "data": "my message"})
        assert answered_prompts == {"my message": "Here's my answer."}

    def test_flash(self):
        queue = Queue()
        logger = QueueLogger(queue, "item's name", None, None)
        logger.flash.emit()
        assert queue.get() == ("flash", {"item_name": "item's name"})
