from spine_engine.project_item.executable_item_base import ExecutableItemBase


class ExecutableItem(ExecutableItemBase):
    @staticmethod
    def item_type():
        return "TestItem"
