import datetime
import unittest
from unittest.mock import patch

from pendulum import Date, DateTime
from timetables.biweekly_timetable import BiweeklyTimetable

from airflow.timetables.base import DataInterval, TimeRestriction


def date_with_start_time(year: int, month: int, day: int, with_tz: bool = False) -> DateTime:
    time = BiweeklyTimetable.send_time
    if with_tz:
        return DateTime.combine(Date(year, month, day), time).replace(tzinfo=datetime.timezone.utc)
    return DateTime.combine(Date(year, month, day), time)


class TestBiweeklyTimetable(unittest.TestCase):

    def setUp(self):
        self.tt = BiweeklyTimetable()

    def test_get_next_workday_next_start_holiday(self):
        next_start = DateTime(2025, 12, 25)
        assert BiweeklyTimetable().get_next_workday(next_start) == DateTime(2025, 12, 26, 0, 0, 0)

    def test_get_next_workday_next_start_weekend(self):
        next_start = DateTime(2025, 11, 30)
        assert BiweeklyTimetable().get_next_workday(next_start) == DateTime(2025, 12, 1, 0, 0, 0)

    def test_get_next_workday_next_start_recursive(self):
        # next_start is a weekend, next_start += 1 is a holiday
        next_start = DateTime(2023, 12, 24)
        assert BiweeklyTimetable().get_next_workday(next_start) == DateTime(2023, 12, 26, 0, 0, 0)
        # next_start is a holiday, next_start += 1 is a weekend
        next_start = DateTime(2020, 12, 25)
        assert BiweeklyTimetable().get_next_workday(next_start) == DateTime(2020, 12, 28, 0, 0, 0)

    def test_get_next_workday_next_start_good(self):
        next_start = DateTime(2025, 12, 1)
        assert BiweeklyTimetable().get_next_workday(next_start) == next_start

    def test_next_dagrun_info_has_previous_run_normal_schedule(self):
        """
        last_automated_data_interval.start is a Monday, no adjustment necessary
        to find next_start.
        """
        restriction = TimeRestriction(earliest=DateTime(2025, 12, 1), latest=None, catchup=False)
        last_run_normal = DataInterval(
            start=date_with_start_time(2025, 11, 17),
            end=date_with_start_time(2025, 12, 1),
        )
        next_run = self.tt.next_dagrun_info(
            last_automated_data_interval=last_run_normal, restriction=restriction
        )
        assert next_run.data_interval.start == date_with_start_time(2025, 12, 1)

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
        next_run = self.tt.next_dagrun_info(
            last_automated_data_interval=last_run_on_tuesday, restriction=restriction
        )
        assert next_run.data_interval.start == date_with_start_time(2025, 12, 1)

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
        restriction = TimeRestriction(earliest=DateTime(2025, 12, 1), latest=None, catchup=False)
        assert self.tt.next_dagrun_info(
            last_automated_data_interval=None, restriction=restriction
        ).data_interval.start == DateTime.combine(Date.today(), self.tt.send_time)

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
            ).data_interval.start == date_with_start_time(2025, 12, 1)

    def test_next_dagrun_info_use_earliest(self):
        """
        No previous run, TimeRestriction.catchup=True, and TimeRestriction.earliest
        is after today, send at TimeRestriction.earliest (at scheduled send_time).
        """
        future_date = DateTime(2025, 12, 1, 15, 0, 0)
        with patch(
            "timetables.biweekly_timetable.Date.today",
            return_value=Date(2025, 11, 30),
        ):
            assert self.tt.next_dagrun_info(
                last_automated_data_interval=None,
                restriction=TimeRestriction(earliest=future_date, latest=None, catchup=True),
            ).data_interval.start == DateTime.combine(future_date.date(), self.tt.send_time)

    def test_next_dagrun_info_use_today(self):
        """
        No previous run, TimeRestriction.catchup=True, and TimeRestriction.earliest
        is before today, send today (at scheduled send_time).
        """
        past_date = DateTime(2025, 12, 1, 15, 0, 0)
        assert self.tt.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=TimeRestriction(earliest=past_date, latest=None, catchup=True),
        ).data_interval.start == DateTime.combine(past_date.date(), self.tt.send_time)

    def test_next_dagrun_info_before_restriction_latest(self):
        """
        next_start is before TimeRestriction.latest, return DagRunInfo.
        """
        next_run = self.tt.next_dagrun_info(
            last_automated_data_interval=DataInterval(
                start=date_with_start_time(2025, 11, 17),
                end=date_with_start_time(2025, 11, 17),
            ),
            restriction=TimeRestriction(
                earliest=None,
                latest=DateTime.combine(Date.today(), self.tt.send_time),
                catchup=True,
            ),
        )
        assert next_run.data_interval.start == date_with_start_time(2025, 12, 1)

    def test_next_dagrun_info_past_restriction_latest(self):
        """
        next_start is after TimeRestriction.latest, return None.
        """
        assert (
            self.tt.next_dagrun_info(
                last_automated_data_interval=DataInterval(
                    start=DateTime.combine(Date.today(), self.tt.send_time),
                    end=DateTime.combine(Date.today(), self.tt.send_time),
                ),
                restriction=TimeRestriction(
                    earliest=None,
                    latest=DateTime.combine(Date.today().add(days=-4), self.tt.send_time),
                    catchup=True,
                ),
            )
            == None
        )

    def test_adjust_run_to_target_weekday_no_adjustment(self):
        """
        last_automated_data_interval.start is a Monday, do not adjust.
        """
        restriction = TimeRestriction(earliest=DateTime(2025, 12, 1), latest=None, catchup=False)
        last_run_normal = DataInterval(
            start=date_with_start_time(2025, 11, 17),
            end=date_with_start_time(2025, 12, 1),
        )
        next_run = self.tt.next_dagrun_info(
            last_automated_data_interval=last_run_normal, restriction=restriction
        )
        assert next_run.data_interval.start == date_with_start_time(2025, 12, 1)

    def test_adjust_time(self):
        assert self.tt.adjust_time(DateTime(2025, 1, 1, 0, 0, 0)) == date_with_start_time(
            2025, 1, 1
        )
        assert self.tt.adjust_time(DateTime(2025, 1, 1, 16, 0, 0)) == date_with_start_time(
            2025, 1, 1
        )

    def test_adjust_run_to_target_weekday_adjust_to_previous(self):
        """
        last_automated_data_interval.start is a Tuesday, adjust to previous Monday.
        """
        restriction = TimeRestriction(earliest=DateTime(2025, 12, 1), latest=None, catchup=False)
        last_run_tuesday = DataInterval(
            start=date_with_start_time(2025, 11, 18),
            end=date_with_start_time(2025, 12, 2),
        )
        next_run = self.tt.next_dagrun_info(
            last_automated_data_interval=last_run_tuesday, restriction=restriction
        )
        assert next_run.data_interval.start == date_with_start_time(2025, 12, 1)

    def test_adjust_run_to_target_weekday_adjust_to_next(self):
        """
        last_automated_data_interval.start is a Friday, adjust to next Monday.
        """
        restriction = TimeRestriction(earliest=DateTime(2025, 12, 1), latest=None, catchup=False)
        last_run_friday = DataInterval(
            start=date_with_start_time(2025, 11, 21),
            end=date_with_start_time(2025, 12, 5),
        )
        next_run = self.tt.next_dagrun_info(
            last_automated_data_interval=last_run_friday, restriction=restriction
        )
        assert next_run.data_interval.start == date_with_start_time(2025, 12, 8)

    def test_infer_manual_data_interval(self):
        """
        Creates an interval for manually triggered runs.
        Interval will start on a workday, but not
        necessarily a Monday (automated scheduler can figure
        that out next time).
        """
        # Creates interval without adjustment
        assert self.tt.infer_manual_data_interval(
            run_after=DateTime(2025, 12, 2, 13, 5, 49)
        ) == DataInterval(
            start=date_with_start_time(2025, 11, 18),
            end=date_with_start_time(2025, 12, 2),
        )
        # Creates interval with adjustment because start falls on a holiday.
        assert self.tt.infer_manual_data_interval(
            run_after=DateTime(2026, 1, 8, 13, 5, 49)
        ) == DataInterval(
            start=date_with_start_time(2025, 12, 26),
            end=date_with_start_time(2026, 1, 9),
        )
