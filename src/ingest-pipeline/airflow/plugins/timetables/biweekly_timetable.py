import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from pendulum import UTC, Date, DateTime, Time
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
    send_time = Time(16, 0, 0)

    def get_next_workday(self, next_start: DateTime) -> DateTime:
        holidays = (
            holiday_calendar.holidays(start=next_start, end=next_start).to_pydatetime()
            if holiday_calendar
            else set()
        )
        # ensure next_start does not fall on weekend or holiday;
        # increment next_start by 1 day until valid day is found
        if next_start.weekday() in (5, 6) or next_start in holidays:
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
        if (
            last_automated_data_interval is not None
        ):  # There was a previous run on the regular schedule.
            adjusted_last_start = self.adjust_run_to_target_weekday(last_automated_data_interval)
            last_start = adjusted_last_start.start
            next_start = DateTime.combine(
                (last_start + timedelta(days=self.run_interval_days)).date(), self.send_time
            )
        # Otherwise this is the first ever run on the regular schedule...
        elif (earliest := restriction.earliest) is None:
            return None  # No start_date. Don't schedule.
        elif not restriction.catchup:
            # If the DAG has catchup=False, today is the earliest to consider.
            next_start = max(earliest, DateTime.combine(Date.today(), self.send_time))
        elif earliest.time() != self.send_time:
            # If earliest is before 16:00:00 UTC, set to 16.
            # If earliest is between 16:00:01 and 19:00:00, send at 19:00:00.
            # If earliest is after 19:00:01, skip to next day.
            if earliest.time() < self.send_time:
                next_start = DateTime.combine(earliest.date(), self.send_time)
            elif self.send_time < earliest.time() <= Time(19, 0, 0):
                next_start = DateTime.combine(earliest.date(), Time(19, 0, 0))
            else:
                next_start = DateTime.combine(earliest.date() + timedelta(days=1), self.send_time)
        else:
            next_start = earliest
        # Skip weekends and holidays
        next_start = self.get_next_workday(next_start)

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(
            start=next_start, end=(next_start + timedelta(days=self.run_interval_days))
        )

    def adjust_run_to_target_weekday(
        self, last_run: DataInterval, target_weekday: WeekDay = WeekDay.MONDAY
    ):
        if last_run.start.weekday() == target_weekday:
            return last_run
        elif last_run.start.weekday() in [WeekDay.TUESDAY, WeekDay.WEDNESDAY]:
            previous_monday = last_run.start.previous(WeekDay.MONDAY, keep_time=True)
            last_run = DataInterval(start=previous_monday, end=last_run.end)
        else:
            next_monday = last_run.start.next(WeekDay.MONDAY, keep_time=True)
            last_run = DataInterval(start=next_monday, end=last_run.end)
        return last_run

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        """
        Used for manually triggered runs, where `run_after` == when triggered
        """
        # Set start to <self.run_interval_days> ago at <self.send_time> o'clock.
        start = DateTime.combine(
            (run_after - timedelta(self.run_interval_days)).date(), self.send_time
        ).replace(tzinfo=UTC)
        # Retrace steps that would have happened to get actual last run datetime.
        start = self.get_next_workday(start)
        # Return actual last start incremented by <self.run_interval_days>.
        return DataInterval(start=start, end=(start + timedelta(days=self.run_interval_days)))


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [BiweeklyTimetable]
