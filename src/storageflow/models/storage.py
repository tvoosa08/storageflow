from abc import ABC, abstractmethod
from typing import List


class Storage(ABC):
    @abstractmethod
    def list_files(self, path: str) -> List[str]:
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass

    @abstractmethod
    def read_text(self, path: str) -> str:
        pass
