import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from pendulum import Date, DateTime, Time
from pendulum.day import WeekDay

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable

if TYPE_CHECKING:
    from airflow.timetables.base import TimeRestriction

log = logging.getLogger(__name__)
try:
    from pandas.tseries.holiday import USFederalHolidayCalendar

    holiday_calendar = USFederalHolidayCalendar()
except ImportError:
    log.warning("Could not import pandas. Holidays will not be considered.")
    holiday_calendar = None  # type: ignore[assignment]


class BiweeklyTimetable(Timetable):
    """
    Runs biweekly on Mondays.
    If Monday is a holiday, increments += 1 until it finds a non-holiday.

    Cribbed from https://airflow.apache.org/docs/apache-airflow/2.11.0/howto/timetable.html
    """

    run_interval_days = 14
    send_time = Time(16, 0, 0)  # 11am Eastern
    cutoff_time = Time(20, 0, 0)

    def get_next_workday(self, next_start: DateTime) -> DateTime:
        holidays = (
            holiday_calendar.holidays(start=next_start, end=next_start).to_pydatetime()
            if holiday_calendar
            else set()
        )
        # ensure next_start does not fall on weekend or holiday;
        # increment next_start by 1 day until valid day is found
        if next_start.day_of_week in [WeekDay.SATURDAY, WeekDay.SUNDAY] or next_start in holidays:
            next_start_incremented = next_start.add(days=1)
            # must be recursive to deal with back-to-back holidays/weekend days
            new_start = self.get_next_workday(next_start_incremented)
            return new_start
        return next_start

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: "TimeRestriction",
    ) -> DagRunInfo | None:
        # There was a previous run on the regular schedule;
        # increment by self.run_interval_days to find next_start.
        if last_automated_data_interval is not None:
            adjusted_last_start = self.adjust_run_to_monday(last_automated_data_interval)
            last_start = adjusted_last_start.start
            next_start = DateTime.combine(
                (last_start + timedelta(days=self.run_interval_days)).date(), self.send_time
            )
        # No previous run and restriction.earliest=None; don't schedule.
        elif restriction.earliest is None:
            return None
        # No previous run and catchup=False; next_start is the later of
        # restriction.earliest and today.
        elif not restriction.catchup:
            next_start = max(restriction.earliest, DateTime.combine(Date.today(), self.send_time))
        # No previous run, restriction.earliest is set, catchup=True;
        # next_start must be set to restriction.earliest.
        else:
            next_start = restriction.earliest

        # Make sure start time matches self.start_time
        next_start = self.adjust_time(next_start)
        # Skip weekends and holidays
        next_start = self.get_next_workday(next_start)

        # After the DAG's scheduled end; don't schedule.
        if restriction.latest is not None and next_start > restriction.latest:
            return None

        return DagRunInfo.interval(
            start=next_start, end=(next_start + timedelta(days=self.run_interval_days))
        )

    def adjust_time(self, date_to_adjust: DateTime):
        return DateTime.combine(date_to_adjust.date(), self.send_time)

    def adjust_run_to_monday(self, run: DataInterval):
        if run.start.day_of_week in [WeekDay.TUESDAY, WeekDay.WEDNESDAY, WeekDay.THURSDAY]:
            previous_monday = run.start.previous(WeekDay.MONDAY, keep_time=True)
            run = DataInterval(start=previous_monday, end=run.end)
        elif run.start.day_of_week is not WeekDay.MONDAY:
            next_monday = run.start.next(WeekDay.MONDAY, keep_time=True)
            run = DataInterval(start=next_monday, end=run.end)
        return run

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        """
        Used for manually triggered runs, where run_after == when triggered.
        """
        # Infer start to be <self.run_interval_days> before <run_after>
        start = DateTime.combine((run_after - timedelta(self.run_interval_days)).date(), Time.min)
        # Adjust start if inferred value is not a workday
        start = DateTime.combine(self.get_next_workday(start), self.send_time)
        return DataInterval(start=start, end=(start + timedelta(days=self.run_interval_days)))


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [BiweeklyTimetable]
