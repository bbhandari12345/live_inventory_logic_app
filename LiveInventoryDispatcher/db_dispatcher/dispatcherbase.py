from LiveInventoryDispatcher.base.base import *


class DispatcherBase(Base):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.object_type = ObjectType.DISPATCHER

    def execute(self) -> Any:
        return self.load_data() \
                    .dispatch()

    @abstractmethod
    def load_data(self) -> Any:
        pass

    @abstractmethod
    def dispatch(self) -> Any:
        pass
