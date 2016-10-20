from collections import defaultdict


def tree():
    return defaultdict(tree)


class Country(object):
    def __init__(self, country_iso):
        print 'Creating country object for {0}'.format(country_iso)

        self.iso = country_iso

        self.rows = tree()
        self.year_list = []
        self.state_dict = {}
        self.week_dict = {}

    def add_year(self, year):
        if year not in self.year_list:
            self.year_list.append(year)

    def add_week(self, year, week_num):

        try:
            self.week_dict[year]
        except KeyError:
            self.week_dict[year] = []

        if week_num not in self.week_dict[year]:
            self.week_dict[year].append(week_num)

    def add_state(self, year, state_id):
        try:
            self.state_dict[year]
        except KeyError:
            self.state_dict[year] = []

        if state_id not in self.state_dict[year]:
            self.state_dict[year].append(state_id)

    def add_row(self, row):

        year = row['year']
        week = row['week']
        state = row['state_iso']

        self.add_week(year, week)
        self.add_state(year, state)
        self.add_year(year)

        self.rows[year][state][week] = row

    def add_dummy_rows(self):

        for y in self.year_list:
            for s in self.state_dict[y]:
                for w in self.week_dict[y]:

                    if w in self.rows[y][s]:
                        pass
                    else:
                        # print '{0} {1} is missing data for state {2}, week {3}'.format(self.iso, y, s, w)
                        self.add_dummy_row(y, s, w)

    def add_dummy_row(self, year, state, week):

        dummydict = {u'week': week, u'year': year, u'state_iso': state, u'country_iso': self.iso,
                     u'loss': 0.0, u'alerts': 0, u'above_ground_carbon_loss': 0.0,
                     u'confidence': 3}

        weeknum = self.weekoffset(state, week, year)

        # If there is a valid week replacement, use it
        if weeknum:
            dummydict['cumulative_emissions'] = self.rows[year][state][weeknum]['cumulative_emissions']
            dummydict['cumulative_deforestation'] = self.rows[year][state][weeknum]['cumulative_deforestation']
            dummydict['percent_to_deforestation_target'] = \
                self.rows[year][state][weeknum]['percent_to_deforestation_target']
            dummydict['percent_to_emissions_target'] = \
                self.rows[year][state][weeknum]['percent_to_emissions_target']

        # If not, i.e. we've tried weeks 3, 2, 1, 53 . . .
        # then we can assume there are no alerts for this week
        else:
            dummydict['cumulative_emissions'] = 0.0
            dummydict['cumulative_deforestation'] = 0.0
            dummydict['percent_to_deforestation_target'] = 0.0
            dummydict['percent_to_emissions_target'] = 0.0

        self.rows[year][state][week] = dummydict

    def to_rows(self):

        row_list = []

        for year, state_dict in self.rows.iteritems():
            for state, week_dict in state_dict.iteritems():
                for week, row in week_dict.iteritems():
                    row_list.append(row)

        return row_list

    def weekoffset(self, state, weeknum, year):
        first_week_dict = {2015: 0, 2016: 53}
        # We can't replace data for week 53
        if weeknum == 53 and year == 2016:
            new_week = None
        else:
            start_week = weeknum - 1
            for w in range(start_week, -1, -1):
                # If we've already tried week #1, try #53
                if w == 0 and year == 2016:
                    w = 53
                # If we have valid data for this week, great
                # Leave our for loop and return the new week value
                if w in self.rows[year][state]:
                    new_week = w
                    break
                # If we don't have valid data, continue with the for loop
                # until we find it
                else:
                    # If we're trying the very first week of the year, and that still
                    # fails, then nothing possible
                    if w == first_week_dict[year]:
                        new_week = None
                        break
                    else:
                        pass
        return new_week