import datetime
import unittest
from unittest.mock import patch

from pendulum import Date, DateTime, Time, day
from timetables.biweekly_timetable import (  # type: ignore
    BiweeklyTimetable,
    TestTargetDayTimetable,
)

from airflow.timetables.base import DataInterval, TimeRestriction

tz: datetime.timezone = datetime.timezone.utc


def date_with_start_time(
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
    date: Date | None = None,
    time: Time | None = None,
    date_time: DateTime | None = None,
    with_tz: bool = False,
) -> DateTime:
    # Must have one source of date
    if not date and not all([year, month, day]) and not date_time:
        raise
    elif all([year, month, day]):
        assert year and month and day  # satisfying type-hinting
        date = Date(year, month, day)
    elif date_time:
        date = date_time.date()
    # Set to default send_time
    if not time and not date_time:
        time = BiweeklyTimetable.send_time
    elif date_time:
        time = date_time.time()
    assert date and time
    if with_tz:
        return DateTime.combine(date, time, tzinfo=tz)
    return DateTime.combine(date, time)


class TestBiweeklyTimetable(unittest.TestCase):

    def setUp(self):
        self.tt = BiweeklyTimetable()

    def test_get_next_workday_next_start_holiday(self):
        next_start = DateTime(2025, 12, 25, tzinfo=tz)
        assert BiweeklyTimetable().get_next_workday(next_start) == DateTime(
            2025, 12, 26, 0, 0, 0, tzinfo=tz
        )

    def test_get_next_workday_next_start_weekend(self):
        next_start = DateTime(2025, 11, 30, tzinfo=tz)
        assert BiweeklyTimetable().get_next_workday(next_start) == DateTime(
            2025, 12, 1, 0, 0, 0, tzinfo=tz
        )

    def test_get_next_workday_next_start_recursive(self):
        # next_start is a weekend, next_start += 1 is a holiday
        next_start = DateTime(2023, 12, 24, tzinfo=tz)
        assert BiweeklyTimetable().get_next_workday(next_start) == DateTime(
            2023, 12, 26, 0, 0, 0, tzinfo=tz
        )
        # next_start is a holiday, next_start += 1 is a weekend
        next_start = DateTime(2020, 12, 25, tzinfo=tz)
        assert BiweeklyTimetable().get_next_workday(next_start) == DateTime(
            2020, 12, 28, 0, 0, 0, tzinfo=tz
        )

    def test_get_next_workday_next_start_good(self):
        next_start = DateTime(2025, 12, 1, tzinfo=tz)
        assert BiweeklyTimetable().get_next_workday(next_start) == next_start

    def test_next_dagrun_info_has_previous_run_normal_schedule(self):
        """
        last_automated_data_interval.start is a Monday, no adjustment necessary
        to find next_start.
        """
        restriction = TimeRestriction(
            earliest=DateTime(2025, 12, 1, tzinfo=tz), latest=None, catchup=False
        )
        last_run_normal = DataInterval(
            start=date_with_start_time(2025, 11, 17, with_tz=True),
            end=date_with_start_time(2025, 12, 1, with_tz=True),
        )
        with patch(
            "timetables.biweekly_timetable.Date.today",
            return_value=Date(2025, 12, 1),
        ):
            with patch(
                "timetables.biweekly_timetable.DateTime.utcnow",
                return_value=DateTime(2025, 12, 1, 15, 0, 0, tzinfo=tz),
            ):
                next_run = self.tt.next_dagrun_info(
                    last_automated_data_interval=last_run_normal, restriction=restriction
                )
                assert next_run.data_interval.start == date_with_start_time(
                    2025, 12, 1, with_tz=True
                )

    def test_next_dagrun_info_has_previous_run_off_schedule(self):
        """
        last_automated_data_interval.start is a Tuesday, adjust last start date
        to previous Monday and then find next_start.
        """
        restriction = TimeRestriction(earliest=DateTime(2025, 12, 1), latest=None, catchup=False)
        last_run_on_tuesday = DataInterval(
            start=date_with_start_time(2025, 11, 18),
            end=date_with_start_time(2025, 12, 2),
        )
        with patch(
            "timetables.biweekly_timetable.Date.today",
            return_value=Date(2025, 12, 1),
        ):
            with patch(
                "timetables.biweekly_timetable.DateTime.utcnow",
                return_value=DateTime(2025, 12, 1, 15, 0, 0, tzinfo=tz),
            ):
                next_run = self.tt.next_dagrun_info(
                    last_automated_data_interval=last_run_on_tuesday, restriction=restriction
                )
                assert next_run.data_interval.start == date_with_start_time(
                    2025, 12, 1, with_tz=True
                )

    def test_next_dagrun_info_no_earliest(self):
        """
        No previous run and no TimeRestriction.earliest,
        should not be scheduled. Return None.
        """
        restriction = TimeRestriction(
            earliest=None, latest=date_with_start_time(2025, 1, 1), catchup=False
        )
        assert (
            self.tt.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
            == None
        )

    def test_next_dagrun_info_no_catchup_today(self):
        """
        No previous run and TimeRestriction.catchup=False,
        TimeRestriction.earliest is earlier than today, send today.
        """
        restriction = TimeRestriction(
            earliest=DateTime(2025, 12, 1, tzinfo=tz), latest=None, catchup=False
        )
        with patch(
            "timetables.biweekly_timetable.Date.today",
            return_value=Date(2025, 12, 2),
        ):
            with patch(
                "timetables.biweekly_timetable.DateTime.utcnow",
                return_value=DateTime(2025, 12, 2, 15, 0, 0, tzinfo=tz),
            ):
                assert self.tt.next_dagrun_info(
                    last_automated_data_interval=None, restriction=restriction
                ).data_interval.start == date_with_start_time(date=Date.today(), with_tz=True)

    def test_next_dagrun_info_no_catchup_earliest(self):
        """
        No previous run and TimeRestriction.catchup=False,
        TimeRestriction.earliest is later than today, send
        at TimeRestriction.earliest.
        """
        with patch(
            "timetables.biweekly_timetable.Date.today",
            return_value=Date(2025, 11, 30),
        ):
            restriction = TimeRestriction(
                earliest=date_with_start_time(2025, 12, 1), latest=None, catchup=False
            )
            assert self.tt.next_dagrun_info(
                last_automated_data_interval=None, restriction=restriction
            ).data_interval.start == date_with_start_time(2025, 12, 1, with_tz=True)

    restriction_date = DateTime(2025, 12, 1, 15, 0, 0, tzinfo=tz)

    def test_next_dagrun_info_use_earliest(self):
        """
        No previous run, TimeRestriction.catchup=True, and TimeRestriction.earliest
        is after today, send at TimeRestriction.earliest (at scheduled send_time).
        """
        with patch(
            "timetables.biweekly_timetable.Date.today",
            return_value=Date(2025, 11, 30),
        ):
            assert self.tt.next_dagrun_info(
                last_automated_data_interval=None,
                restriction=TimeRestriction(
                    earliest=self.restriction_date, latest=None, catchup=True
                ),
            ).data_interval.start == date_with_start_time(
                date=self.restriction_date.date(), with_tz=True
            )

    def test_next_dagrun_info_use_today(self):
        """
        No previous run, TimeRestriction.catchup=True, and TimeRestriction.earliest
        is before today, send today (at scheduled send_time).
        """
        assert self.tt.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=TimeRestriction(earliest=self.restriction_date, latest=None, catchup=True),
        ).data_interval.start == date_with_start_time(
            date=self.restriction_date.date(), with_tz=True
        )

    def test_next_dagrun_info_before_restriction_latest(self):
        """
        next_start is before TimeRestriction.latest, return DagRunInfo.
        """
        next_run = self.tt.next_dagrun_info(
            last_automated_data_interval=DataInterval(
                start=date_with_start_time(2025, 11, 17),
                end=date_with_start_time(2025, 12, 1),
            ),
            restriction=TimeRestriction(
                earliest=None,
                latest=date_with_start_time(date=Date.today(), with_tz=True),
                catchup=True,
            ),
        )
        assert next_run.data_interval.start == date_with_start_time(2025, 12, 1, with_tz=True)

    def test_next_dagrun_info_past_restriction_latest(self):
        """
        next_start is after TimeRestriction.latest, return None.
        """
        assert (
            self.tt.next_dagrun_info(
                last_automated_data_interval=DataInterval(
                    start=date_with_start_time(date=Date.today()),
                    end=date_with_start_time(date=Date.today()),
                ),
                restriction=TimeRestriction(
                    earliest=None,
                    latest=date_with_start_time(date=Date.today().add(days=-4), with_tz=True),
                    catchup=True,
                ),
            )
            == None
        )

    def test_adjust_run_to_target_weekday_no_adjustment(self):
        """
        last_automated_data_interval.start is a Monday, do not adjust.
        """
        restriction = TimeRestriction(
            earliest=DateTime(2025, 12, 1, tzinfo=tz),
            latest=None,
            catchup=False,
        )
        last_run_normal = DataInterval(
            start=date_with_start_time(2025, 11, 17),
            end=date_with_start_time(2025, 12, 1),
        )
        with patch(
            "timetables.biweekly_timetable.Date.today",
            return_value=Date(2025, 12, 1),
        ):
            with patch(
                "timetables.biweekly_timetable.DateTime.utcnow",
                return_value=DateTime(2025, 12, 1, 14, 0, 0, tzinfo=tz),
            ):
                next_run = self.tt.next_dagrun_info(
                    last_automated_data_interval=last_run_normal, restriction=restriction
                )
                assert next_run.data_interval.start == date_with_start_time(
                    2025, 12, 1, with_tz=True
                )

    def test_adjust_time(self):
        with patch(
            "timetables.biweekly_timetable.Date.today",
            return_value=Date(2025, 12, 1),
        ):
            with patch(
                "timetables.biweekly_timetable.DateTime.utcnow",
                return_value=DateTime(2025, 12, 1, 14, 0, 0, tzinfo=tz),
            ):
                # No adjustment (16:00:00 -> 16:00:00)
                valid_time = date_with_start_time(2025, 12, 1, with_tz=True)
                assert self.tt.adjust_time(valid_time) == valid_time
                # Adjust send_time only (15:00:00 -> 16:00:00)
                assert (
                    self.tt.adjust_time(DateTime(2025, 12, 1, 15, 0, 0, tzinfo=tz)) == valid_time
                )
            with patch(
                "timetables.biweekly_timetable.DateTime.utcnow",
                return_value=DateTime(2025, 12, 1, 17, 0, 0, tzinfo=tz),
            ):
                # Adjust to next day at send_time (12-01 17:00:00 -> 12-02 16:00:00)
                assert self.tt.adjust_time(
                    DateTime(2025, 12, 1, 21, 0, 0)
                ) == date_with_start_time(2025, 12, 2, with_tz=True)
                # Date is in the future, just adjust send_time
                assert self.tt.adjust_time(
                    DateTime(2025, 12, 5, 21, 0, 0)
                ) == date_with_start_time(2025, 12, 5, with_tz=True)

    def test_adjust_run_to_target_weekday_adjust_to_previous(self):
        """
        last_automated_data_interval.start is a Tuesday, adjust to previous Monday.
        """
        last_run_tuesday = DataInterval(
            start=date_with_start_time(2025, 11, 18),
            end=date_with_start_time(2025, 12, 2),
        )
        adjusted_run = self.tt.adjust_run_to_target_weekdays(run=last_run_tuesday)
        assert adjusted_run.start == date_with_start_time(2025, 11, 17, with_tz=True)
        assert adjusted_run.end == date_with_start_time(2025, 12, 1, with_tz=True)

    def test_adjust_run_to_target_weekday_adjust_to_next(self):
        """
        last_automated_data_interval.start is a Friday, adjust to next Monday.
        """
        last_run_friday = DataInterval(
            start=date_with_start_time(2025, 11, 21),
            end=date_with_start_time(2025, 12, 5),
        )
        adjusted_run = self.tt.adjust_run_to_target_weekdays(run=last_run_friday)
        assert adjusted_run.start == date_with_start_time(2025, 11, 24, with_tz=True)
        assert adjusted_run.end == date_with_start_time(2025, 12, 8, with_tz=True)

    def test_infer_manual_data_interval(self):
        """
        Creates an interval for manually triggered runs.
        Does not adjust based on weekdays or holidays.
        """
        manual_run = DateTime(2025, 12, 2, 13, 5, 49)
        assert self.tt.infer_manual_data_interval(run_after=manual_run) == DataInterval(
            start=date_with_start_time(
                date_time=manual_run.add(days=-self.tt.run_interval_days), with_tz=True
            ),
            end=date_with_start_time(date_time=manual_run, with_tz=True),
        )

    def test_sequence_of_intervals(self):
        restriction = TimeRestriction(earliest=DateTime(2025, 12, 1), latest=None, catchup=False)
        # start with an off-schedule manual run on a Tuesday at wrong time
        run_after = DateTime(2025, 12, 2, 13, 5, 49)
        manual_interval = self.tt.infer_manual_data_interval(run_after=run_after)
        assert manual_interval.start == date_with_start_time(
            date_time=run_after.add(days=-self.tt.run_interval_days),
            with_tz=True,
        )
        assert manual_interval.end == date_with_start_time(date_time=run_after, with_tz=True)
        assert manual_interval.start.day_of_week == day.WeekDay.TUESDAY
        # use that DataInterval when calculating next_dagrun_info
        with patch(
            "timetables.biweekly_timetable.DateTime.utcnow",
            return_value=DateTime(2025, 12, 1, 15, 0, 0, tzinfo=tz),
        ):
            with patch(
                "timetables.biweekly_timetable.Date.today",
                return_value=Date(2025, 12, 1),
            ):
                next_auto_run = self.tt.next_dagrun_info(
                    last_automated_data_interval=manual_interval, restriction=restriction
                )
                # assert that schedule is back on track
                assert next_auto_run.data_interval.start == date_with_start_time(
                    2025, 12, 1, with_tz=True
                )
                assert next_auto_run.data_interval.end == date_with_start_time(
                    2025, 12, 15, with_tz=True
                )
                assert next_auto_run.data_interval.start.day_of_week == day.WeekDay.MONDAY
                assert next_auto_run.data_interval.end.day_of_week == day.WeekDay.MONDAY


class TestTestTargetDayTimetable(unittest.TestCase):
    def test_adjust_run(self):
        tt = TestTargetDayTimetable()
        last_run_monday = DataInterval(
            start=date_with_start_time(2025, 11, 17),
            end=date_with_start_time(2025, 12, 1),
        )
        assert tt.adjust_run_to_target_weekdays(last_run_monday).start == last_run_monday.start
        last_run_wednesday = DataInterval(
            start=date_with_start_time(2025, 11, 19),
            end=date_with_start_time(2025, 12, 3),
        )
        assert tt.adjust_run_to_target_weekdays(last_run_wednesday).start == date_with_start_time(
            2025, 11, 20
        )
        last_run_friday = DataInterval(
            start=date_with_start_time(2025, 11, 21),
            end=date_with_start_time(2025, 12, 5),
        )
        assert tt.adjust_run_to_target_weekdays(last_run_friday).start == date_with_start_time(
            2025, 11, 24
        )
