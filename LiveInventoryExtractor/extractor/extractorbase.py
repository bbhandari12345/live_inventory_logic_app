from LiveInventoryExtractor.base.base import *


class ExtractorBase(Base):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.object_type = ObjectType.EXTRACTOR

    def execute(self) -> any:
        return self.fetch_config(). \
                    transform_data(). \
                    write(data_file_dir = self.kwargs['extractor_write_path'])

    @abstractmethod
    def fetch_config(self) -> any:
        pass

    @abstractmethod
    def transform_data(self) -> any:
        pass