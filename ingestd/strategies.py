import sys
import types
import collections
import itertools
import re

from ingestd.sources import Source


if sys.version_info[0] > 2:
    createBoundMethod = types.MethodType
else:
    def createBoundMethod(func, obj):
        return types.MethodType(func, obj, obj.__class__)


class RecordParsingStrategy:
    def __init__(self, strategyName='default', replacementFn=None):
        self.name = strategyName
        if replacementFn:
            self.execute = createBoundMethod(replacementFn, self)

    def execute(self):
        pass


def parseXML(self):
    if isinstance(self, Source):
        for record in self.stream():
            yield record


def parseDelimited(self, delimiter: str = None):
    if isinstance(self, Source):

        if delimiter is not None:
            _delimiter = delimiter
        else:
            _nonalnum = collections.deque()
            for char in self.stream().__next__():
                if not char.isalnum():
                    _nonalnum.append(char)
            _delimiter = tuple(collections.Counter(_nonalnum))[0]

        for record in self.stream():
            yield record.rstrip().split(sep=_delimiter)
    else:
        raise ValueError(f'Incorrect type, {self} is not an instance of the Source class.')


def parseFixedWidth(self):

    _fixedwidth_lkup = {}

    def classify(row: str):
        row_type = re.findall(pattern='(SEC|FIN|CMP)', string=row)[0]
        if row_type in {'SEC','FIN', 'CMP'}:
            return row_type

    if isinstance(self, Source):
        for record in self.stream():
            record_type = classify(record)
            _cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in _fixedwidth_lkup.get(record_type)))
            _pads = tuple(fw < 0 for fw in _fixedwidth_lkup.get(record_type))
            _fields = tuple(itertools.zip_longest(_pads, (0, ) + _cuts, _cuts))[:-1]
            fn = lambda line: tuple(line[i: j] for pad, i, j in _fields if not pad)
            yield record_type, fn(record)
    else:
        raise ValueError(f'Incorrect type, {self} is not an instance of the Operator class.')