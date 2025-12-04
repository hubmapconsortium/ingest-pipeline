import datetime
import unittest

from pendulum import Date, DateTime, Time
from timetables.biweekly_timetable import BiweeklyTimetable

from airflow.timetables.base import DataInterval, TimeRestriction


class TestBiweeklyTimetable(unittest.TestCase):
    """
    16:00:00 UTC == 11:00:00 Eastern
    """

    interval = BiweeklyTimetable.run_interval_days

    def date_with_start_time(
        self, year: int, month: int, day: int, no_tz: bool = True
    ) -> DateTime:
        if no_tz:
            return DateTime(year, month, day, 16, 0, 0)
        return DateTime(year, month, day, 16, 0, 0).replace(tzinfo=datetime.timezone.utc)

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

    def test_next_dagrun_info_has_previous_run(self):
        tt = BiweeklyTimetable()
        restriction = TimeRestriction(earliest=DateTime(2025, 12, 1), latest=None, catchup=False)
        # Where 2025/11/17 and 2025/12/01 are Mondays
        last_run_normal = DataInterval(
            start=self.date_with_start_time(2025, 11, 17),
            end=self.date_with_start_time(2025, 12, 1),
        )
        next_run = tt.next_dagrun_info(
            last_automated_data_interval=last_run_normal, restriction=restriction
        )
        # Start date should stay the same
        assert next_run.data_interval.start == self.date_with_start_time(2025, 12, 1)
        # Where 2025/11/18 and 2025/12/2 are Tuesdays
        last_run_not_monday = DataInterval(
            start=self.date_with_start_time(2025, 11, 18),
            end=self.date_with_start_time(2025, 12, 2),
        )
        next_run = tt.next_dagrun_info(
            last_automated_data_interval=last_run_not_monday, restriction=restriction
        )
        # Start date should roll back one day to a Monday
        assert next_run.data_interval.start == self.date_with_start_time(2025, 12, 1)

    def test_next_dagrun_info_first_run(self):
        tt = BiweeklyTimetable()
        restriction = TimeRestriction(earliest=None, latest=None, catchup=False)
        assert (
            tt.next_dagrun_info(last_automated_data_interval=None, restriction=restriction) == None
        )

    def test_next_dagrun_info_no_catchup(self):
        tt = BiweeklyTimetable()
        restriction = TimeRestriction(earliest=DateTime(2025, 12, 1), latest=None, catchup=False)
        assert tt.next_dagrun_info(
            last_automated_data_interval=None, restriction=restriction
        ).data_interval.start == DateTime.combine(Date.today(), tt.send_time)

    def test_next_dagrun_info_catchup_too_early(self):
        tt = BiweeklyTimetable()
        before_send_time = DateTime(2025, 12, 1, 15, 0, 0)
        assert tt.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=TimeRestriction(earliest=before_send_time, latest=None, catchup=True),
        ).data_interval.start == DateTime.combine(before_send_time.date(), tt.send_time)

    def test_next_dagrun_info_catchup_within_bounds(self):
        tt = BiweeklyTimetable()
        within_send_time = DateTime(2025, 12, 1, 18, 0, 0)
        assert tt.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=TimeRestriction(earliest=within_send_time, latest=None, catchup=True),
        ).data_interval.start == DateTime.combine(within_send_time.date(), Time(19, 0, 0))

    def test_next_dagrun_info_catchup_too_late(self):
        tt = BiweeklyTimetable()
        after_send_time = DateTime(2025, 12, 1, 20, 0, 0)
        assert tt.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=TimeRestriction(earliest=after_send_time, latest=None, catchup=True),
        ).data_interval.start == DateTime.combine(
            after_send_time.date().add(days=1), Time(16, 0, 0)
        )

    def test_next_dagrun_before_restriction_latest(self):
        tt = BiweeklyTimetable()
        assert tt.next_dagrun_info(
            last_automated_data_interval=DataInterval(
                start=DateTime(2025, 11, 17, 16, 0, 0),
                end=DateTime(2025, 11, 17, 16, 0, 0),
            ),
            restriction=TimeRestriction(
                earliest=None,
                latest=DateTime.combine(Date.today(), tt.send_time),
                catchup=True,
            ),
        ).data_interval.start == DateTime(2025, 12, 1, 16, 0, 0)

    def test_next_dagrun_info_past_restriction_latest(self):
        tt = BiweeklyTimetable()
        assert (
            tt.next_dagrun_info(
                last_automated_data_interval=DataInterval(
                    start=DateTime.combine(Date.today(), tt.send_time),
                    end=DateTime.combine(Date.today(), tt.send_time),
                ),
                restriction=TimeRestriction(
                    earliest=None,
                    latest=DateTime.combine(Date.today().add(days=-4), tt.send_time),
                    catchup=True,
                ),
            )
            == None
        )

    def test_adjust_run_to_target_weekday(self):
        pass

    def test_infer_manual_data_interval(self):
        # ?
        pass
