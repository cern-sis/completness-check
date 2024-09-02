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

    @arg_botcmd("--from-date", dest="from_date", type=str, unpack_args=False)
    @arg_botcmd("--to-date", dest="to_date", type=str, default=None)
    def arxiv_completeness(self, msg, from_date, to_date):
        """
        Command that retrieves information regarding the harvesting between two dates
        """
        parsed_from_date = datetime.strptime(from_date, "%d-%m-%Y")
        if to_date is not None:
            parsed_to_date = datetime.strptime(to_date, "%d-%m-%Y")
        else:
            parsed_from_date = date.today()

        yield "Arxiv Completeness Check may take some time, please be patient"
        yield arxiv_completness_check_script.completeness_check(
            parsed_from_date, parsed_to_date
        )
