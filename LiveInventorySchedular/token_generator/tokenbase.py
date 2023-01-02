from LiveInventorySchedular.base.base import *


class TokenGenBase(Base):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.object_type = ObjectType.GENERATOR

    @abstractmethod
    def fetch_config(self) -> Any:
        pass

    @abstractmethod
    def fetch_vendor_data(self) -> Any:
        pass

    @abstractmethod
    def load_data(self) -> Any:
        pass

    @abstractmethod
    def dispatch(self) -> Any:
        pass

    def execute(self) -> Any:
        return self.fetch_config(). \
            fetch_vendor_data(). \
            load_data(). \
            dispatch()
