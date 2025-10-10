from enum import Enum

class FilterOperator(str, Enum):
    """
    Defines the types of filter operations that can be specified in the configuration.
    Inherits from 'str' so that the enum members are also string instances,
    making them directly usable where a string is expected (e.g., serialization).
    """

    EQUALS = "Equals"  # Checks for equality against a single value.
    EXISTS = "Exists"  # Checks if a property exists (is not null).
    CONTAINSALL = "ContainsAll"  # Checks if an item contains all specified values for a given property
    IN = "In"  # Checks if a value is within a list of specified values. Not implementing CONTAINSANY b/c IN is usually more suitable
    SEARCH = "Search"  # Performs full text search on a specified property

