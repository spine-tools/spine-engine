def item_specification_factories():
    from .test_item.specification_factory import SpecificationFactory

    return {"TestItem": SpecificationFactory}


def executable_items():
    from .test_item.executable_item import ExecutableItem

    return {"TestItem": ExecutableItem}
