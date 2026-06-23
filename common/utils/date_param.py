from datetime import datetime, date, timedelta

class DateParamBuild:
    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date

    def make_date_list(self) -> list[str]:
        start = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(self.end_date, "%Y-%m-%d").date()
        today = date.today()

        if start > end:
            raise ValueError("시작일자는 종료일자보다 늦을 수 없습니다.")

        if end > today:
            raise ValueError("종료일자는 오늘 날짜보다 뒤일 수 없습니다.")

        date_lst = []
        current = start

        while current <= end:
            date_lst.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        return date_lst