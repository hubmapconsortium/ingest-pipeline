import logging
from datetime import timedelta, timezone
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
    If Monday is a holiday, increments hours += 24 until it finds a non-holiday.

    Cribbed from https://airflow.apache.org/docs/apache-airflow/2.11.0/howto/timetable.html
    """

    tz: timezone = timezone.utc
    run_interval_days: int = 14
    send_time: Time = Time(16, 0, 0, tzinfo=tz)  # 11am Eastern
    cutoff_time: Time = Time(20, 0, 0, tzinfo=tz)
    target_weekdays: list[WeekDay] = [WeekDay.MONDAY]

    def get_next_workday(self, next_start: DateTime) -> DateTime:
        # Holiday calendar requires tz-aware datetime
        if not next_start.tzinfo:
            next_start = DateTime.combine(next_start.date(), next_start.time(), tzinfo=self.tz)
        holidays = (
            holiday_calendar.holidays(start=next_start, end=next_start).to_pydatetime()
            if holiday_calendar
            else set()
        )
        # ensure next_start does not fall on weekend or holiday;
        # increment next_start by 24 hours (hours=24 rather than days=1 prevents removal of tz)
        # until valid day is found
        if next_start.day_of_week in [WeekDay.SATURDAY, WeekDay.SUNDAY] or next_start in holidays:
            next_start_incremented = next_start.add(hours=24)
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
            adjusted_last_start = self.adjust_run_to_target_weekdays(
                last_automated_data_interval, self.target_weekdays
            )
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
            next_start = max(
                restriction.earliest,
                DateTime.combine(Date.today(), self.send_time, tzinfo=self.tz),
            )
        # No previous run, restriction.earliest is set, catchup=True;
        # next_start must be set to restriction.earliest.
        else:
            next_start = restriction.earliest

        # Make sure start time is valid
        next_start = self.adjust_time(next_start, catchup=bool(restriction.catchup))
        # Skip weekends and holidays
        next_start = self.get_next_workday(next_start)

        # After the DAG's scheduled end; don't schedule.
        if restriction.latest is not None and next_start > restriction.latest:
            return None

        return DagRunInfo.interval(
            start=next_start, end=(next_start + timedelta(days=self.run_interval_days))
        )

    def adjust_time(self, date_to_adjust: DateTime, catchup: bool = False) -> DateTime:
        if catchup == True:
            return DateTime.combine(date_to_adjust.date(), self.send_time, tzinfo=self.tz)
        if not date_to_adjust.tzinfo:
            date_to_adjust = DateTime.combine(
                date_to_adjust.date(), date_to_adjust.time(), tzinfo=self.tz
            )
        todays_send_time = DateTime.combine(Date.today(), self.send_time, tzinfo=self.tz)
        if date_to_adjust.date() < DateTime.utcnow().date():
            # date_to_adjust date has already passed, advance send_date to tomorrow
            date = Date.today().add(days=1)
        elif date_to_adjust.date() == DateTime.utcnow().date():
            # but we're past the send_time for today, send tomorrow
            if DateTime.utcnow() > todays_send_time:
                date = Date.today().add(days=1)
            else:
                date = Date.today()
        else:
            date = date_to_adjust.date()
        return DateTime.combine(date, self.send_time, tzinfo=self.tz)

    def adjust_run_to_target_weekdays(
        self, run: DataInterval, target_weekdays: list[WeekDay] = [WeekDay.MONDAY]
    ) -> DataInterval:
        del target_weekdays
        return self.adjust_run_to_monday(run)

    def adjust_run_to_monday(self, run: DataInterval) -> DataInterval:
        if run.start.day_of_week is WeekDay.MONDAY:
            return run
        if run.start.day_of_week in [WeekDay.TUESDAY, WeekDay.WEDNESDAY, WeekDay.THURSDAY]:
            start = run.start.previous(WeekDay.MONDAY, keep_time=True)
            end = run.end.add(days=-start.diff(run.start).days)
        else:
            start = run.start.next(WeekDay.MONDAY, keep_time=True)
            end = run.end.add(days=start.diff(run.start).days)
        return DataInterval(start=start, end=end)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        """
        Used for manually triggered runs, where run_time == when triggered.
        """
        return DataInterval(
            start=(
                DateTime.combine(
                    run_after - timedelta(self.run_interval_days),
                    Time(run_after.hour, run_after.minute, run_after.second),
                    tzinfo=self.tz,
                )
            ),
            end=DateTime.combine(run_after.date(), run_after.time(), tzinfo=self.tz),
        )


class TestSendTimetable(BiweeklyTimetable):
    """
    Just test that daily send works.
    """

    run_interval_days = 1

    def adjust_run_to_target_weekdays(self, run, target_weekdays=[WeekDay.MONDAY]) -> DataInterval:
        del target_weekdays
        return run


class TestTargetDayTimetable(BiweeklyTimetable):
    """
    Test that adjust_run_to_target_weekdays works.
    """

    run_interval_days = 1

    def adjust_run_to_target_weekdays(
        self,
        run,
        target_weekdays=[WeekDay.MONDAY, WeekDay.TUESDAY, WeekDay.THURSDAY],
    ) -> DataInterval:
        while run.start.day_of_week not in target_weekdays:
            run = DataInterval(start=run.start.add(days=1), end=run.end.add(days=1))
        return run


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [BiweeklyTimetable]


class TestSendTimetablePlugin(AirflowPlugin):
    name = "test_timetable_plugin"
    timetables = [TestSendTimetable]


class TestTargetDayTimetablePlugin(AirflowPlugin):
    name = "test_timetable_plugin"
    timetables = [TestTargetDayTimetable]
