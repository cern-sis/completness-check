from datetime import date, datetime, timedelta

from errbot import BotPlugin, arg_botcmd

from lib import arxiv_completness_check_script

TODAY = date.today()
if TODAY.weekday() == 0:
    DEFAULT_FROM_DATE = TODAY - timedelta(days=3)
else:
    DEFAULT_FROM_DATE = TODAY - timedelta(days=1)


class ArxivCompleteness(BotPlugin):
    """
    Arxiv Completeness Errbot Plugin

    """

    @arg_botcmd("--from-date", dest="from_date", type=str, default=None)
    @arg_botcmd("--to-date", dest="to_date", type=str, default=None)
    def arxiv(self, msg, from_date, to_date):
        """
        Command that retrieves information regarding the harvesting between two dates
        """
        if from_date is not None:
            from_date = datetime.strptime(from_date, "%d-%m-%Y").date()
        else:
            from_date = DEFAULT_FROM_DATE
        if to_date is not None:
            to_date = datetime.strptime(to_date, "%d-%m-%Y").date()
        else:
            to_date = TODAY

        yield "Arxiv Completeness Check may take some time, please be patient"
        yield arxiv_completness_check_script.completeness_check(from_date, to_date)
