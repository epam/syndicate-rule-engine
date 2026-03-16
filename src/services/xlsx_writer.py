"""
This is a wrapper over xlsxwriter. It handles such difficulties:
- skipping empty columns: in case the number of empty cells in a specific
  column exceeds a limit, the whole column will be skipped automatically.
  By, default the limit is 1.
- merging cells: in case some columns of a specific row contain more cells
  than other columns of the same row, all the others will be merged.
- provides more or less convenient interface:
>>> ft: Format  # some workbook format
>>> wsh: Worksheet
>>> table = Table()
>>> table.new_row()
>>> table.add_cells(CellContent('№'))
>>> table.add_cells(CellContent('Resource', ft))
>>> table.add_cells(CellContent('Rules', ft))
>>> table.new_row()
>>> table.add_cells(CellContent(1))
>>> table.add_cells(CellContent({'arn': 'arn:aws:test'}))
>>> table.add_cells(CellContent('ecc-001'), CellContent('ecc-002'))
>>> XlsxRowsWriter().write(wsh, table)

This will result in such a table:

+----+-------------------------+---------+
| №  |        Resource         |  Rules  |
+----+-------------------------+---------+
| 1  | {"arn": "arn:aws:test"} | ecc-001 |
|    |                         +---------+
|    |                         | ecc-002 |
+----+-------------------------+---------+

See, how merging of cells automatically appeared. It happend because we
put two arguments into add_cells() method.

"""

import json

from xlsxwriter.format import Format
from xlsxwriter.worksheet import Worksheet

from helpers import skip_indexes


class Cell:
    __slots__ = ('row', 'col')

    def __init__(self, row: int = 0, col: int = 0):
        self.row = row
        self.col = col

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(row={self.row}, col={self.col})'


class CellContent:
    __slots__ = ('dt', 'ft')

    def __init__(self, dt: int | float | str | dict | list | None = None,
                 ft: Format | None = None):
        self.dt = dt
        self.ft = ft

    @property
    def data(self) -> str:
        if isinstance(self.dt, str):
            return self.dt
        return json.dumps(self.dt, default=str)

    def __bool__(self) -> bool:
        return self.dt is not None

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(dt={self.dt}, ft={self.ft})'


# inner tuple cannot contain Nones. ONLY CellContent.
Row = list[tuple[CellContent, ...]]


class Table:
    __slots__ = ('buffer', )

    def __init__(self, buffer: list[Row] | None = None):
        self.buffer: list[Row] = buffer or list()

    def new_row(self):
        """
        Creates a new raw
        :return:
        """
        self.buffer.append(list())

    def add_cells(self, *args: CellContent):
        """
        Adds new cells to the current row
        :param args:
        :return:
        """
        self.buffer[-1].append(tuple(args))  # make sure you called new_row

    @property
    def rows(self) -> int:
        return len(self.buffer)

    @property
    def cols(self) -> int:
        return len(max(self.buffer, key=len))


class XlsxRowsWriter:
    def __init__(self, empty_col_threshold: int = 1):
        """
        :param empty_col_threshold: number of not empty cells when we still
        consider this column an empty one. Default 1 - for header.
        """
        self._threshold = empty_col_threshold

    def empty_cols(self, rows: list[Row]) -> set[int]:
        """
        :param rows:
        :return:
        """
        empty = set()
        for i, col in enumerate(zip(*rows)):
            if sum(map(any, col)) <= self._threshold:
                empty.add(i)
        return empty

    @staticmethod
    def _write_row(row: Row, wsh: Worksheet, pointer: Cell,
                   empty: set[int]):
        """

        :param row:
        :param wsh:
        :param pointer:
        :param empty:
        :return:
        """
        highest = len(max(skip_indexes(row, empty), key=len, default=()))

        for i, col in enumerate(skip_indexes(row, empty)):
            for j, cell in enumerate(col):
                if cell.ft:
                    wsh.write(pointer.row + j, pointer.col + i, cell.data,
                              cell.ft)
                else:
                    wsh.write(pointer.row + j, pointer.col + i, cell.data)
            if highest > 1 and highest > len(col) > 0:  # need merge
                wsh.merge_range(
                    pointer.row + j,
                    pointer.col + i,
                    pointer.row + j + highest - len(col),
                    pointer.col + i,
                    ''
                )
        pointer.row += highest

    def write(self, wsh: Worksheet, table: Table, start: Cell | None = None):
        # todo allow to pass iterator of Rows of CellContent instead of table
        pointer = start or Cell()
        rows = table.buffer
        empty = self.empty_cols(rows)
        for row in rows:
            self._write_row(row, wsh, pointer, empty)
