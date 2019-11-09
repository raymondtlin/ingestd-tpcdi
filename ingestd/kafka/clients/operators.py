import itertools
import re
from abc import ABC, abstractmethod


class OperatorFactory:
    """
    Factory Object
    """
    def __init__(self):
        self._creators = {}

    def register_format(self, format, creator):
        self._creators[format] = creator

    def get_operator(self, format):
        creator = self._creators.get(format)
        if not creator:
            raise ValueError(format)
        return creator


class Operator(ABC):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_format = file_path.split('\\.')[-1]

    def create_handle(self):
        with open(self.file_path, 'r') as handle:
            yield handle.readlines()

    @abstractmethod
    def parse(self):
        pass


class XMLOperator(Operator):
    def __init__(self, file_path):
        super().__init__(file_path)

    def parse(self):
        pass


class DelimitedOperator(Operator):
    """
    Operator to handle delimited files
    """
    def __init__(self, file_path, delimiter):
        super().__init__(file_path)
        self.delimiter = delimiter

    def parse(self):
        for record in self.create_handle():
            for itm in record.split(self.delimiter):
                yield itm


class FixedWidthOperator(Operator):
    """
    Operator to handle fixed-width format files
    """
    def __init__(self, file_path=None, field_widths=None):
        super().__init__(file_path)
        self.field_widths = field_widths or {}

    def classify_record(self, record: str):
        """
        Searches for the 3-character document type in the record
        :param: record: str
        :return: record_type: str # One of ("SEC","CMP","FIN")
        """
        token = re.compile(r'(SEC|CMP|FIN)')
        record_type = re.findall(token, record)[0]

        if record_type in ('SEC', 'FIN', 'CMP'):
            return record_type
        else:
            print('Invalid return value from classify_record method')
            raise ValueError

    def create_parser(self):
        cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in self.field_widths))
        pads = tuple(fw < 0 for fw in self.field_widths)
        fields = tuple(itertools.zip_longest(pads, (0, ) + cuts, cuts))[:-1]
        return lambda line: tuple(line[i: j] for pad, i, j in fields if not pad)

    def parse(self):
        try:
            for record in self.create_handle():
                p = self.create_parser()(record)
                yield p
        except BaseException as e:
            print(f"Exception in record parsing {e}")