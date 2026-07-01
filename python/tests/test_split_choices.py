import ibis

from alaska_legislative_data import _split_choices


def test_add_member_id_includes_former_codes():
    """Votes cast under a former code (eg before a name change) still resolve."""
    members = ibis.memtable(
        {
            "LegislatureNumber": [34, 34],
            "MemberCode": ["FRB", "KAW"],
            "MemberFormerCodes": ["BUR", None],
            "MemberId": ["34:H:40:Robyn Burke:34", "34:S:P:Scott Kawasaki:25"],
        }
    )
    choices = ibis.memtable(
        {
            "LegislatureNumber": [34, 34, 34],
            "MemberCode": ["BUR", "FRB", "KAW"],
        }
    )
    result = _split_choices._add_member_id(choices, members).execute()
    by_code = dict(zip(result.MemberCode, result.MemberId))
    assert by_code == {
        "BUR": "34:H:40:Robyn Burke:34",
        "FRB": "34:H:40:Robyn Burke:34",
        "KAW": "34:S:P:Scott Kawasaki:25",
    }


def test_add_member_id_splits_multiple_former_codes():
    members = ibis.memtable(
        {
            "LegislatureNumber": [34],
            "MemberCode": ["CCC"],
            "MemberFormerCodes": ["AAA;BBB"],
            "MemberId": ["34:H:1:Some Person:34"],
        }
    )
    choices = ibis.memtable(
        {
            "LegislatureNumber": [34, 34, 34],
            "MemberCode": ["AAA", "BBB", "CCC"],
        }
    )
    result = _split_choices._add_member_id(choices, members).execute()
    assert set(result.MemberId) == {"34:H:1:Some Person:34"}
    assert len(result) == 3
