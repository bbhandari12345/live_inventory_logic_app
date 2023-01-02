from LiveInventorySchedular.base.base import *


class SchedulerBase(Base):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.vendor_ids_to_sync = None
        self.internal_ids_to_sync = None
        # self.object_type = ObjectType.Scheduler

    def execute(self) -> Any:
        if self.vendor_ids_to_sync or self.internal_ids_to_sync:
            return [], \
                self.generate_fetcher_sync_command()  # If we are overriding vendor ids, we just need to run this

        # Vendor id override not being done, run the whole thing
        return self.fetch_sync_candidates_priority().generate_fetcher_sync_priority_command(), \
            self.fetch_sync_candidates().generate_fetcher_sync_command()

    @abstractmethod
    def fetch_sync_candidates(self) -> Any:
        pass

    @abstractmethod
    def fetch_sync_candidates_priority(self) -> Any:
        pass

    @abstractmethod
    def generate_fetcher_sync_command(self) -> Any:
        pass

    @abstractmethod
    def generate_fetcher_sync_priority_command(self) -> Any:
        pass

    @abstractmethod
    def generate_access_token_cmd_for_sync_candidates(self) -> Any:
        pass
