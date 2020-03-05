#!/usr/bin/env python3

import sys
import types
import collections
import itertools
import re

from lxml import etree


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
    def parse_from_unicode(unicode_str):
        parser = etree.XMLParser(encoding='UTF-8', remove_blank_text=True)
        s = unicode_str.encode('UTF-8')
        return etree.fromstring(s, parser)

    root = parse_from_unicode(open(self.file_path).read())

    for elem in root:
        record_items = []

        for subelem in elem.find('*'):
            if subelem:
                attrs = [dict({child_elem.tag: child_elem.text}) for child_elem in subelem.iter()]
            if attrs:
                record_items.append(attrs)

        return tuple(elem.attrib, record_items)


def parseDelimited(self, delimiter: str = None):
    """

    :param self: FileSource object instance
    :param delimiter: single character string
    :return:
    """
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


def parseFixedWidth(self):
    """

    :param self: FileSource object instance
    :return:
    """

    _fixedwidth_lkup = {"CMP": (15, 3, 60, 10, 4, 2, 4, 8, 80, 80, 12, 25, 20, 24, 46, 150),
                        "SEC": (15, 3, 15, 6, 4, 706, 13, 8, 8, 12, 60),
                        "FIN": (15, 3, 4, 1, 8, 8, 17, 17, 12, 12, 12, 17, 17, 17, 13, 13, 60)}

    def classify(row: str):
        row_type = re.findall(pattern='(SEC|FIN|CMP)', string=row)[0]
        if row_type in {'SEC', 'FIN', 'CMP'}:
            return row_type

    for record in self.stream():
        record_type = classify(record)
        _cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in _fixedwidth_lkup.get(record_type)))
        _pads = tuple(fw < 0 for fw in _fixedwidth_lkup.get(record_type))
        _fields = tuple(itertools.zip_longest(_pads, (0, ) + _cuts, _cuts))[:-1]
        fn = lambda line: tuple(line[i: j] for pad, i, j in _fields if not pad)
        yield record_type, fn(record)
