import os
import sys
import json
import datetime
import pandas as pd
import subprocess
import errno
import log
import boto3


def load_json_from_token(file_name):

    root_dir = sys.prefix
    token_file = os.path.join(root_dir, "tokens", file_name)

    with open(token_file) as data_file:
        data = json.load(data_file)

    return data


def hadoopresult_to_df(result_csv, dataset_tech_name):

    terra_i_alerts_fields = [
        "year",
        "julian_day",
        "polyname",
        "bound1",
        "bound2",
        "iso",
        "adm1",
        "adm2",
        "alerts",
    ]

    umd_landsat_alerts_fields = [
        "confidence",
        "year",
        "julian_day",
        "area_m2",
        "above_ground_carbon_loss",
        "climate_mask",
        "polyname",
        "bound1",
        "bound2",
        "iso",
        "adm1",
        "adm2",
        "alerts",
    ]

    fire_alerts_fields = [
        "alert_date",
        "fire_type",
        "polyname",
        "bound1",
        "bound2",
        "iso",
        "adm1",
        "adm2",
        "alerts",
    ]

    analysis_fields_dict = {
        "terra_i_alerts": terra_i_alerts_fields,
        "umd_landsat_alerts": umd_landsat_alerts_fields,
        "fires_report": fire_alerts_fields,
        "fires_country_pages": fire_alerts_fields,
    }

    # read in field names
    field_names = analysis_fields_dict[dataset_tech_name]

    # csv to pandas data frame
    df = pd.read_csv(result_csv, names=field_names)

    # convert string date to dt object
    log.info("Converting date string to object")
    if dataset_tech_name in ["umd_landsat_alerts", "terra_i_alerts"]:
        # create column alert_date from julian_day and year
        # previously we would do df.apply with our julian_date_from_yrday function
        # but given the size of some of the input dataframes, we're running into memory errors

        # instead let's group by year + julian_day, then convert that subset to datetime
        date_group_df = df.groupby(["year", "julian_day"]).size().reset_index()

        # now look up these unique combinations to datetime
        date_group_df["alert_date"] = date_group_df.apply(
            lambda row: julian_date_from_yrday(row), axis=1
        )

        # delete extra size row
        del date_group_df[0]

        # join back to main df
        df = pd.merge(df, date_group_df, on=["year", "julian_day"])

    else:
        df.alert_date = pd.to_datetime(
            df.alert_date, infer_datetime_format=True, format="%Y/%m/%d"
        )

    # fill blank bounds - can cause issues if we groupby null values
    df.bound1 = df.bound1.fillna(1)
    df.bound2 = df.bound2.fillna(1)

    return df


def write_outputs(results_df, output_s3_path, environment):

    root_dir = sys.prefix
    results_dir = os.path.join(root_dir, "results")
    mkdir_p(results_dir)

    basename = os.path.basename(output_s3_path)
    log.info("output s3 path: {}".format(output_s3_path))

    results_csv = os.path.join(results_dir, basename)

    # write to csv
    results_df.to_csv(results_csv, index=False)

    # push to aws
    if environment in ["prod", "staging"]:
        cmd = ["aws", "s3", "cp", results_csv, output_s3_path]
        subprocess.check_call(cmd)


def julian_date_from_yrday(row):
    alert_year = row["year"]
    j_day = int(row["julian_day"])
    alert_date = datetime.datetime(alert_year, 1, 1) + datetime.timedelta(j_day)

    return alert_date


def mkdir_p(path):
    # copied from https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def update_status(status, test=True):
    s3 = boto3.resource("s3")
    status_file = s3.Object(
        "gfw2-data", "forest_change/umd_landsat_alerts/prod/events/status"
    )

    if not test:
        status_file.put(Body=status)
    return
