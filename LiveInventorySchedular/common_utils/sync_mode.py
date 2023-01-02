"""
this is enum class for different execution mode we have:
=> for schedular run , specify empty
=> ondemand prefix is ONDEMAND_
"""
from enum import Enum


class PROGRAM_EXECUTION_MODE(Enum):
    ONDEMAND = "ondemand_"

    def __str__(self):
        return str(self.value)
