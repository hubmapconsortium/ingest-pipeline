import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from pendulum import UTC, Date, DateTime, Time

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
    # TODO: needs thorough checking
    """
    Cribbed from https://airflow.apache.org/docs/apache-airflow/2.11.0/howto/timetable.html
    """

    incr = 14

    def get_next_workday(self, d: DateTime) -> DateTime:
        next_start = d
        while True:
            # ensure next_start does not fall on weekend or holiday
            if next_start.weekday() not in (5, 6):
                if holiday_calendar is None:
                    holidays = set()
                else:
                    holidays = holiday_calendar.holidays(
                        start=next_start, end=next_start
                    ).to_pydatetime()
                if next_start not in holidays:
                    break
            next_start = next_start.add(days=-self.incr)
        return next_start

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        """
        Used for manually triggered runs, where `run_after` == when triggered
        """
        start = DateTime.combine((run_after - timedelta(self.incr)).date(), Time.min).replace(
            tzinfo=UTC
        )
        # Skip backwards over weekends and holidays to find last run
        start = self.get_next_workday(start)
        return DataInterval(start=start, end=(start + timedelta(days=self.incr)))

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if (
            last_automated_data_interval is not None
        ):  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            next_start = DateTime.combine(
                (last_start + timedelta(days=self.incr)).date(), Time.min
            )
        # Otherwise this is the first ever run on the regular schedule...
        elif (earliest := restriction.earliest) is None:
            return None  # No start_date. Don't schedule.
        elif not restriction.catchup:
            # If the DAG has catchup=False, today is the earliest to consider.
            next_start = max(earliest, DateTime.combine(Date.today(), Time.min))
        elif earliest.time() != Time.min:
            # If earliest does not fall on midnight, skip to the next day.
            next_start = DateTime.combine(earliest.date() + timedelta(days=self.incr), Time.min)
        else:
            next_start = earliest
        # Skip weekends and holidays
        next_start = self.get_next_workday(next_start.replace(tzinfo=UTC))

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=(next_start + timedelta(days=self.incr)))


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [BiweeklyTimetable]
