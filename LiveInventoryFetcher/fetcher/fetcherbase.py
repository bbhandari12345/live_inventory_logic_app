from LiveInventoryFetcher.base.base import *


class FetcherBase(Base):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.object_type = ObjectType.FETCHER
        self.summary = {}
        # declaration of variable that holds the list dictionary of error and error description status for each vendor
        self.vendor_codes_error_status = None

    @abstractmethod
    def fetch_config(self) -> Any:
        pass

    @abstractmethod
    def fetch_vendor_data(self) -> Any:
        pass

    def execute(self) -> Any:
        return self.fetch_config(). \
                    fetch_vendor_data(). \
                    write(data_file_dir = self.kwargs['fetcher_write_path'])

    def execution_summary(self)->Any:
        return self.summary

