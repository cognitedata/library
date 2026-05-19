class FilterParser:
    """Parses and evaluates filters against given items."""

    def __init__(self, filter_config: dict) -> None:
        """Initializes the FilterParser with a filter configuration.

        Args:
            filter_config (dict): Dictionary representing the filter configuration.
        """
        self.filter_config = filter_config

    def matches(self, item: dict) -> bool:
        """Checks if the given item matches the filter configuration.

        Args:
            item (dict): Dictionary representing the item to evaluate.

        Returns:
            bool: True if the item matches the filter, False otherwise.
        """
        return self._evaluate_filter(self.filter_config, item)

    def _evaluate_filter(self, filter_section: dict, item: dict) -> bool:
        """Recursively evaluates the filter section against the item.

        Args:
            filter_section (dict): Current section of the filter to evaluate.
            item (dict): Dictionary representing the item to evaluate.

        Returns:
            bool: True if the item matches the filter section, False otherwise.

        Raises:
            ValueError: If the filter structure is unsupported.
        """
        matches = []
        for key, sub_filter in filter_section.items():
            match key:
                # TODO: Revisit
                # Order of operations might matter so this is not necessarily the best way to do this
                case "and":
                    matches.append(all(self._eval_multi(sub_filter, item)))
                case "or":
                    matches.append(any(self._eval_multi(sub_filter, item)))
                case "not":
                    matches.append(not self._evaluate_filter(sub_filter, item))
                case "include":
                    matches.append(self._evaluate_include(sub_filter, item))
                case "eq":
                    matches.append(any(item[k] == sub_filter[k] for k in sub_filter))
                case _:
                    raise ValueError(f"Currently unsupported filter structure: {key}")

        return all(matches)

    def _eval_multi(self, filter_: dict, item: dict) -> list[bool]:
        """Evaluates multiple filter sections against the item."""
        matches = []
        for k, v in filter_.items():
            part = {k: v}
            matches.append(self._evaluate_filter(part, item))
        return matches

    def _evaluate_include(self, include_section: dict, item: dict) -> bool:
        """Evaluates the 'include' section of the filter against the item.

        Args:
            include_section (dict): Dictionary representing the 'include' section.
            item (dict): Dictionary representing the item to evaluate.

        Returns:
            bool: True if the item matches the 'include' section, False otherwise.
        """
        return all(item.get(key, None) in values for key, values in include_section.items())
