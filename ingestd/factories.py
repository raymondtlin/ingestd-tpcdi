#!/usr/bin/env python3


class SourceFactory:
    """
    Factory which contains submitted Sources to stream
    """

    def __init__(self):
        self._sources = {}

    def register(self, key, source):
        """
        Registers a stream source
        :param key: identifier String for registered source
        :param source: Source object
        """
        self._sources[key] = source

    def get_source(self, key):
        """
        Retrieves instance of registered source.
        :param key: registered file_format str
        :return: source object
        """
        source = self._sources.get(key)
        if not source:
            raise ValueError(key)
        return source


class StrategyFactory:
    """
    Factory which contains created strategies
    """

    def __init__(self):
        self._strategies = {}

    def register(self, key, strategy):
        """
        Registers an strategy
        :param key: identifier String for registered Strategy
        :param strategy: Strategy
        :return:
        """
        self._strategies[key] = strategy

    def get(self, key):
        """
        Retrieves instance of registered strategy.
        :param key: registered file_format str
        :return: Operator object
        """
        strategy = self._strategies.get(key)
        if not strategy:
            raise ValueError(key)
        return strategy
