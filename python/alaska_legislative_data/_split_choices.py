import ibis
from ibis import _


def split_choices(
    *, choices_raw: ibis.Table, bills: ibis.Table, members: ibis.Table
) -> tuple[ibis.Table, ibis.Table]:
    choices_raw = _add_bill_id(choices_raw, bills)
    choices_raw = _add_member_id(choices_raw, members)

    choices_raw = choices_raw.mutate(
        # VoteNum begins as eg "H0026"
        # VoteCode=_.VoteNum,
        VoteChamber=_.VoteNum[0],
        VoteNumber=_.VoteNum[1:].cast("uint16"),
    ).relocate("VoteChamber", after="LegislatureNumber")
    choices_raw = choices_raw.mutate(
        VoteId=_.LegislatureNumber.cast(str)
        + ":"
        + _.VoteChamber
        + ":"
        + _.VoteNumber.cast(str)
    )
    choices_raw = choices_raw.mutate(
        ChoiceId=_.VoteId + ":" + _.MemberId.re_replace(r"^\d+:", "")
    )

    votes = (
        choices_raw.select(
            "VoteId",
            "LegislatureNumber",
            "VoteChamber",
            "VoteNumber",
            "VoteDate",
            "VoteTitle",
            "BillId",
            "VoteBillAmendmentNumber",
        )
        .distinct()
        .order_by("LegislatureNumber", "VoteChamber", "VoteNumber")
    )
    choices = (
        choices_raw.select(
            "ChoiceId",
            "VoteId",
            "MemberId",
            "Choice",
        )
        .distinct()
        .order_by("ChoiceId")
    )
    return votes, choices


def _add_bill_id(choices: ibis.Table, bills: ibis.Table) -> ibis.Table:
    dupes = (
        bills.group_by("LegislatureNumber", "BillNumber")
        .agg(n=_.count())
        .filter(_.n > 1)
        .execute()
    )
    assert len(dupes) == 0, dupes
    bill_id_lookup = bills.select("LegislatureNumber", "BillNumber", "BillId")
    missing = choices.filter(
        _.LegislatureNumber.notnull(),
        _.BillNumber.notnull(),
        _.LegislatureNumber.notin(bill_id_lookup.LegislatureNumber),
        _.BillNumber.notin(bill_id_lookup.BillNumber),
    )
    assert missing.count().execute() == 0
    return choices.left_join(bill_id_lookup, ["LegislatureNumber", "BillNumber"]).drop(
        "LegislatureNumber_right", "BillNumber_right"
    )


def _add_member_id(choices: ibis.Table, members: ibis.Table) -> ibis.Table:
    # Votes are recorded under whichever code was current when they were cast,
    # so a member with former codes (eg after a name change) needs a lookup
    # entry for each of their codes, all resolving to the same MemberId.
    current_codes = members.select("LegislatureNumber", "MemberCode", "MemberId")
    former_codes = members.filter(_.MemberFormerCodes.notnull()).select(
        "LegislatureNumber",
        MemberCode=_.MemberFormerCodes.split(";").unnest(),
        MemberId=_.MemberId,
    )
    member_id_lookup = current_codes.union(former_codes)
    member_id_lookup = member_id_lookup.filter(
        _.LegislatureNumber.notnull(),
        _.MemberCode.notnull(),
    )
    assert (
        (
            member_id_lookup.group_by("LegislatureNumber", "MemberCode")
            .agg(n=_.count())
            .n
            == 1
        )
        .all()
        .execute()
    )
    # check that every (LegislatureNumber, MemberCode) combo in votes
    # is also present in member_id_lookup
    missing = choices.filter(
        _.LegislatureNumber.notnull(),
        _.MemberCode.notnull(),
        _.LegislatureNumber.notin(member_id_lookup.LegislatureNumber),
        _.MemberCode.notin(member_id_lookup.MemberCode),
    )
    assert missing.count().execute() == 0
    return choices.left_join(
        member_id_lookup, ["LegislatureNumber", "MemberCode"]
    ).drop("LegislatureNumber_right", "MemberCode_right")
