from spine_engine.project_item.project_item_specification_factory import ProjectItemSpecificationFactory


class SpecificationFactory(ProjectItemSpecificationFactory):
    @staticmethod
    def item_type():
        return "TestItem"
