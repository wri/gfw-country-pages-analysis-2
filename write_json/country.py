from collections import defaultdict


def tree():
    return defaultdict(tree)


class Year(object):
    def __init__(self, year_val, iso):
        print 'Creating year object for {0} in {1}'.format(year_val, iso)

        self.iso = iso
        self.year_val = year_val

        self.rows = tree()

        self.state_list = []
        self.week_list = []

    def add_row(self, row):
        week = row['week']
        state = row['state_id']

        self.rows[state][week] = row

        if week not in self.week_list:
            self.week_list.append(week)

        if state not in self.state_list:
            self.state_list.append(state)

    def add_dummy_rows(self):

        for s in self.state_list:
            for w in self.week_list:

                if w in self.rows[s]:
                    pass
                else:
                    # print '{0} {1} is missing data for state {2}, week {3}'.format(self.iso, self.year_val, s, w)
                    self.add_dummy_row(s, w)

    def add_dummy_row(self, state, week):

        dummydict = {u'week': week, u'year': self.year_val, u'state_id': state, u'country_iso': self.iso,
                     u'loss_ha': 0.0, u'alerts': 0, u'above_ground_carbon_loss': 0.0, u'confidence': 3}

        weeknum = weekoffset(self.rows, self.year_val, state, week)

        # If there is a valid week replacement, use it
        if weeknum:
            dummydict['cumulative_emissions'] = self.rows[state][weeknum]['cumulative_emissions']
            dummydict['cumulative_deforestation'] = self.rows[state][weeknum]['cumulative_deforestation']
            dummydict['percent_to_deforestation_target'] = self.rows[state][weeknum]['percent_to_deforestation_target']
            dummydict['percent_to_emissions_target'] = self.rows[state][weeknum]['percent_to_emissions_target']

        # If not, i.e. we've tried weeks 3, 2, 1, 53 . . .
        # then we can assume there are no alerts for this week
        else:
            dummydict['cumulative_emissions'] = 0.0
            dummydict['cumulative_deforestation'] = 0.0
            dummydict['percent_to_deforestation_target'] = 0.0
            dummydict['percent_to_emissions_target'] = 0.0

        self.rows[state][week] = dummydict

    def to_rows(self):
        row_list = []

        for state, week_dict in self.rows.iteritems():
            for week, row in week_dict.iteritems():
                row_list.append(row)

        return row_list


class Country(object):
    def __init__(self, country_iso):
        print 'Creating country object for {0}'.format(country_iso)

        self.iso = country_iso
        self.year_list = []

    def add_row(self, row):

        current_year_list = [year.year_val for year in self.year_list]

        if row['year'] not in current_year_list:
            year = Year(row['year'], self.iso)
            self.year_list.append(year)

        else:
            year = [year for year in self.year_list if year.year_val == row['year']][0]

        year.add_row(row)

    def to_rows(self):

        row_list = []

        for year in self.year_list:

            year.add_dummy_rows()

            row_list.extend(year.to_rows())

        return row_list


def weekoffset(rows, year, state, weeknum):

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
            if w in rows[state]:
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
