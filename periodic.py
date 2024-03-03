from datetime import datetime, time

class periodic:
    def __init__(self, validity, time_of_day_range, days_of_week_range, dates_of_month_range, months_of_year_range, special_keywords):
        self.validity = validity
        self.time_of_day_range = time_of_day_range
        self.days_of_week_range = days_of_week_range
        self.dates_of_month_range = dates_of_month_range
        self.months_of_year_range = months_of_year_range
        self.special_keywords = special_keywords

        if 'any day' in special_keywords or 'full week' in special_keywords:
            self.days_of_week_range = self.days_of_week_range + [0, 1, 2, 3, 4, 5, 6] if self.days_of_week_range else [0, 1, 2, 3, 4, 5, 6]
        if 'weekdays' in special_keywords or 'weekday' in special_keywords:
            self.days_of_week_range = self.days_of_week_range + [1, 2, 3, 4, 5] if self.days_of_week_range else [1, 2, 3, 4, 5]
        if 'weekend' in special_keywords or 'weekends' in special_keywords:
            self.days_of_week_range = self.days_of_week_range + [0, 6] if self.days_of_week_range else [0, 6]
        if any(day in special_keywords for day in ['monday', 'mondays', 'tuesday', 'tuesdays', 'wednesday', 'wednesdays', 'thursday', 'thursdays', 'friday', 'fridays', 'saturday', 'saturdays', 'sunday', 'sundays']):
            day_map = {'monday': 1, 'tuesday': 2, 'wednesday': 3, 'thursday': 4, 'friday': 5, 'saturday': 6, 'sunday': 0}
            for day in special_keywords:
                if day in day_map:
                    self.days_of_week_range = self.days_of_week_range + [day_map[day]] if self.days_of_week_range else [day_map[day]]

        if 'morning' in special_keywords or 'mornings' in special_keywords:
            if not self.time_of_day_range:
                self.time_of_day_range = []
            self.time_of_day_range.append([time(8, 0), time(12, 0)])
        if 'evening' in special_keywords or 'evenings' in special_keywords:
            if not self.time_of_day_range:
                self.time_of_day_range = []
            self.time_of_day_range.append([time(16, 0), time(19, 0)])
        if 'day' in special_keywords or 'days' in special_keywords:
            if not self.time_of_day_range:
                self.time_of_day_range = []
            self.time_of_day_range.append([time(6, 0), time(18, 0)])
        if 'night' in special_keywords or 'nights' in special_keywords:
            if not self.time_of_day_range:
                self.time_of_day_range = []
            self.time_of_day_range.append([time(18, 0), time(6, 0)])
        if 'office' in special_keywords or 'mornings' in special_keywords:
            if not self.time_of_day_range:
                self.time_of_day_range = []
            self.time_of_day_range.append([time(9, 0), time(17, 0)])

        month_map = {'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6, 'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12}
        for month in special_keywords:
            if month in month_map:
                self.months_of_year_range = self.months_of_year_range + [month_map[month]] if self.months_of_year_range else [month_map[month]]

    def satisfies(self, t=None):
        t = t or datetime.now()

        if not self.special_keywords:
            return True

        if self.validity and (t < self.validity[0] or t > self.validity[1]):
            return False

        if self.special_keywords and 'all' in self.special_keywords:
            return True

        if self.days_of_week_range and t.weekday() not in self.days_of_week_range:
            return False

        if self.dates_of_month_range and t.day not in self.dates_of_month_range:
            return False

        if self.months_of_year_range and t.month not in self.months_of_year_range:
            return False

        time_of_day = t.time()
        if self.time_of_day_range:
            valid = False
            for x in self.time_of_day_range:
                st, en = x
                if st <= en:
                    if st <= time_of_day <= en:
                        valid = True
                        break
                else:
                    if st <= time_of_day or time_of_day <= en:
                        valid = True
                        break
            if not valid:
                return False

        return True
