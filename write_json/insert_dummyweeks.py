from country import Country


def insert_dummy_cumulative_rows(record_list):

    country_list = []

    for row in record_list:
        country_list.append(row['country_iso'])

    country_list = list(set(country_list))

    final_row_list = []

    for country_name in country_list:
        country = Country(country_name)

        for row in record_list:
            if row['country_iso'] == country.iso:
                country.add_row(row)

        final_row_list.extend(country.to_rows())

    return final_row_list
