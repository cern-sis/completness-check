from datetime import date, datetime, timedelta

from errbot import BotPlugin, arg_botcmd
from errcron import CrontabMixin

from lib import arxiv_completness_check_script

TODAY = date.today()
if TODAY.weekday() == 0:
    DEFAULT_FROM_DATE = TODAY - timedelta(days=3)
else:
    DEFAULT_FROM_DATE = TODAY - timedelta(days=1)


def get_default_from_date():
    if TODAY.weekday() == 0:
        return TODAY - timedelta(days=3)
    return TODAY - timedelta(days=1)


class ArxivCompleteness(CrontabMixin, BotPlugin):
    """
    Arxiv Completeness Errbot Plugin

    """

    CRONTAB = [" 0 10 * * 1-5 .daily_check"]

    @arg_botcmd("--from-date", dest="from_date", type=str, default=None)
    @arg_botcmd("--to-date", dest="to_date", type=str, default=None)
    def arxiv(self, msg, from_date, to_date):
        """
        Command that retrieves information regarding the harvesting between two dates
        """
        if from_date is not None:
            from_date = datetime.strptime(from_date, "%d-%m-%Y").date()
        else:
            from_date = get_default_from_date()
        if to_date is not None:
            to_date = datetime.strptime(to_date, "%d-%m-%Y").date()
        else:
            to_date = date.today()

        yield "Arxiv Completeness Check may take some time, please be patient"
        yield arxiv_completness_check_script.completeness_check(from_date, to_date)

    def daily_check(self, polled_time):
        client = self._bot.client

        message = arxiv_completness_check_script.completeness_check(
            get_default_from_date(), date.today()
        )

        client.send_message(
            {
                "type": "stream",
                "to": "inspire",
                "topic": "harvest",
                "content": message,
            }
        )
