from unittest.mock import create_autospec, call

import pytest
from xlsxwriter.worksheet import Worksheet

from services.xlsx_writer import CellContent, Table, XlsxRowsWriter, Cell


@pytest.fixture
def table() -> Table:
    t = Table()
    for i in range(3):
        t.new_row()
        t.add_cells(CellContent(i))
        t.add_cells(CellContent(i + 1), CellContent(i + 2))
        t.add_cells(CellContent(i + 3))
        t.add_cells(CellContent(None))

    return t


def test_cell_content():
    c = CellContent('data')
    assert c.data == 'data'
    c = CellContent({'key': 'value'})
    assert c.data == '{"key": "value"}'
    assert bool(c)
    assert not CellContent(None)


def test_table(table):
    assert table.rows == 3
    assert table.cols == 4
    assert len(table.buffer) == 3


def test_write_table(table):
    wsh = create_autospec(Worksheet)
    start = Cell(1, 1)
    writer = XlsxRowsWriter()
    writer.write(wsh, table, start)
    wsh.write.assert_has_calls([
        call(1, 1, '0'),
        call(1, 2, '1'),
        call(2, 2, '2'),
        call(1, 3, '3'),
        call(3, 1, '1'),
        call(3, 2, '2'),
        call(4, 2, '3'),
        call(3, 3, '4'),
        call(5, 1, '2'),
        call(5, 2, '3'),
        call(6, 2, '4'),
        call(5, 3, '5')
    ])
    wsh.merge_range.assert_has_calls([
        call(1, 1, 2, 1, ''),
        call(1, 3, 2, 3, ''),
        call(3, 1, 4, 1, ''),
        call(3, 3, 4, 3, ''),
        call(5, 1, 6, 1, ''),
        call(5, 3, 6, 3, '')
    ])
