import ibis
from ibis import _


def test_bills(bills: ibis.Table):
    assert bills.count().execute() > 0
    id_counts = bills.group_by("SessionNumber", "BillNumber").agg(n=_.count())
    assert id_counts.n.max().execute() == 1
