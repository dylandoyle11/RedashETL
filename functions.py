

#pylint: disable=too-many-arguments
"""
------------------------------------------------------------------------
[Functions]
------------------------------------------------------------------------
Author(s): Dylan Doyle
File Name: Functions.py
Updated: 2021-08-19
------------------------------------------------------------------------

------------------------------------------------------------------------
"""

"""
=============================
Imports
=============================
"""
from datetime import datetime
import dateutil.relativedelta
import os
import pandas as pd
import requests
import time
import math
import shutil
import traceback
import json
import calendar
from io import StringIO

def intro():
    """
    -------------------------------------------------------
    Prints Introduction
    Use: intro()
    -------------------------------------------------------
    Parameters:
       NONE
    Returns:
       NONE
    ------------------------------------------------------
    """

    print("""
            -------------------------
                REDASH REPORTS v2
            -------------------------
              M O T O I N S I G H T
              written by Dylan Doyle
    """)


def _poll_job(s, redash_url, job):
    # Call API endpoint and test connection

    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(1)

    if job['status'] == 3:
        return job['query_result_id']

    return None


def get_report(download_dir, redash_url, report_type, headers, query_id, start, end, report_columns):
    """
    -------------------------------------------------------
    Get reports using the Redash API and filter data
    Use: file_name = getReportAPI(download_dir, url,
        report_type, headers, redash_csv, job_url,
        pass_params, start, end, report_columns)
    -------------------------------------------------------
    Parameters:
       download_dir - Path to 'Exports' directory (Path)
       url - Redash URL (str)
       report_type - Type of report (str)
       headers - Redash Authorization Token Header (dict)
       redash_csv - Redash URL to the report (str)
       job_url - Redash URL to API job (str)
       pass_params - Redash URL to refresh query (str)
       start - Start date (str)
       end - End date (str)
       report_columns - Columns necessary for report (list)
       curr_dir - Path to current directory (Path)
    Returns:
       file_name - Path to newly generated report (str)
    ------------------------------------------------------
    """
    # get current date
    date = datetime.date(datetime.now())

    # download Redash report
    print(f"Exporting [{report_type}] report from {redash_url}...")

    params = {'p_start_date': start,
              'p_end_date': end,
              'max_age': 0}

    s = requests.Session()
    s.headers.update(headers)
    payload = dict(max_age=0, parameters=params)
    response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data=json.dumps(payload, indent=4, sort_keys=True, default=str))

    if response.status_code != 200:
        raise Exception('Refresh failed.')

    # Call API endpoint and repeat if initially failed up to 5 times
    run = False
    attempts = 0
    while run is False:
        if attempts > 4:
            raise exception('Failed to executed query after 5 attempts.')
        result_id = _poll_job(s, redash_url, response.json()['job'])
        if result_id:
            response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
            if response.status_code != 200:
                print('Failed getting results. Retrying...')
            else:
                run = True
        else:
            print('Query execution failed. Retrying...')
        attempts += 1

    # save Redash report
    print('Saving export...')
    if ".ca" in redash_url:
        file_name = os.path.join(download_dir, f'CA_{report_type}_CS Master Report V3 ({start} - {end})_{date}.csv')
    else:
        file_name = os.path.join(download_dir, f'US_{report_type}_CS Master Report V3 ({start} - {end})_{date}.csv')

    # Create DataFrame from json response
    info = json.loads(response.text)
    report_df = pd.json_normalize(info['query_result']['data']['rows'])

    # Save Exports
    report_df.to_csv(file_name, index=None)
    return report_df, file_name


def format_report(report_type, report_df, report_columns, start, filename):
    """
    -------------------------------------------------------

    -------------------------------------------------------
    Parameters:

    ------------------------------------------------------
    """

    # build list of months needed for respective report
    month_list = _get_month_list(report_type, start)
    # drop any rows not in the months needed
    print(month_list)
    try:
        report_df = report_df[report_df['Year/Month::multi-filter'].isin(month_list)]
    except:
        report_df = report_df[report_df['YearMonth'].isin(month_list)]

    # remove all columns from data not in columns_list
    for column in report_df.columns:
        if column not in report_columns:
            report_df = report_df.drop(column, 1)


    # sum rows with same Dealer Salesforce ID row and drop duplicates
    report_df = sum_row_data(report_df)

    # Save filtered report to CSV
    report_df.to_csv(f'{filename} - Filtered.csv', index=False)

    return report_df



def _get_month_list(report_type, start):
    """
    -------------------------------------------------------
    Build filter list of months requested in format "month year", ex: "Jan 2020"
    Use: month_list = get_month_list(report_type, start)
    -------------------------------------------------------
    Parameters:
        report_type - "Y", "M", "W" (String)
        start - start date of report (String)
    Returns:
       month_list - list of months needed in report (List)
    ------------------------------------------------------
    """
    # Only filter one month-year if Weekly or Monthly report is detected

    if report_type == "W" or report_type == "M":
        lim = 1
    # Filter 12 month-year if Yearly report is detected
    elif report_type == "Y":
        lim = 12

    # Generate a list of month-year which will be the filter values
    month_list = []
    # retrieve first month and year requested
    current_year = int(str(start).split("-")[0])
    print(current_year)
    current_month = int(str(start).split("-")[1])
    # loops once if report_type is 1, 12 times if yearly report
    for n in range(lim):
        # takes first 3 letters of month and adds year: January -> Jan 2021
        filter_current = calendar.month_name[current_month][:3] + " " + str(current_year)
        # add to lists of months to filter
        month_list.append(filter_current)

        if report_type == "Y":
            # need more than one month, so must increment a month
            # if month is 12, next month will be 1 in the following year
            # ex: Dec 2019 precedes Jan 2020 (12 2019 -> 1 2020)
            if current_month == 12:
                current_year += 1
                current_month = 1
            # otherwise still in the same year
            else:
                current_month += 1
    return month_list


def sum_row_data(report_df):
    """
    -------------------------------------------------------
    Sum the rows that have the same Dealer Salesforce ID
    Use: report_df = sum_row_data(report_df)
    -------------------------------------------------------
    Parameters:
        report_df - report (Dataframe)
    Returns:
        report_df - transformed report with rows summed (Dataframe)
    ------------------------------------------------------
    """
    print("Summing row data...")
    # save list of columns
    COLUMNS = report_df.columns
    # iterate through columns to sum all instances based on salesforce ID
    for column in COLUMNS:
        # skip summing first 2 columns, sum all others
        if column != 'Dealer Salesforce ID' and column != 'Compulsory':
            report_df[f'{column}2'] = report_df.groupby(['Dealer Salesforce ID'])[column].transform('sum')
            report_df = report_df.drop(column, 1)

    # drop all duplicates
    report_df = report_df.drop_duplicates(subset=['Dealer Salesforce ID'], keep='first')
    new_columns = report_df.columns
    new_columns = [x.strip('2') for x in new_columns]
    report_df.columns = new_columns
    # report_df.columns = COLUMNS

    return report_df



def create_upload(data, template):
    """
    -------------------------------------------------------
    Fill in zeroed out template with data
    Use: template = exportData(data, template)
    -------------------------------------------------------
    Parameters:
       data - All US, CAN data for that report (DataFrame)
       template - Template DataFrame (DataFrame)
    Returns:
       template - Template with all data (DataFrame)
    ------------------------------------------------------
    """
    print("Filling in desired template...")
    # Iterate through data
    rows = len(data.index)

    for i in range(rows):
        dealership = str(data.iat[i, 0])  # Get dealer ID

        # find index of row that matches dealership
        template_row = template.index.values[template['Account ID - 18'] == dealership]
        # returns [13] where 13 is index, [] if doesn't exist

        # empty list returns empty list, so this will only continue if the ID matches
        if len(template_row) > 0:
            template_row = template_row[0]  # is an list of length 1, just make an int

            for j_temp in template.columns:
                if j_temp == 'Account ID - 18':  # skip first column since already in template
                    continue
                elif j_temp == 'Compulsory':  # columns match, so no need to strip ending
                    template.loc[template.index[template_row], j_temp] = data.loc[data.index[i], j_temp]
                else:
                    j_data = j_temp.rsplit(' ', 1)[0]  # strip the LM, MTD or LM at end
                    if j_data == 'Cars Sold':  # inconsistent naming of columns, this handles it
                        j_data = 'Cars Solds'
                    if j_data == 'Trade-Ins':
                        j_data = 'Trade-ins'

                    # transfers data cell to template cell if found in data
                    # if row isn't found, nothing is changed & template row is already zeroed out for all numeric values
                    template.loc[template.index[template_row], j_temp] = data.loc[data.index[i], j_data]
    return template



def get_dates(report_types):
    """
    -------------------------------------------------------
    Get Dates
    Use: list = getDates(report_type, date, useAutoDates)
    -------------------------------------------------------
    Parameters:
       report_type - Type of report (str)
       date - Current date (date)
       useAutoDates - use auto dates if no user input (Boolean)
    Returns:
       list - a list of start and end date (list)
    ------------------------------------------------------
    """
    # Get first day of month and/or month to date data
    dates = []
    date = datetime.date(datetime.now())

    # start date is first of the month
    first = date.replace(day=1)

    for report_type in report_types:

        if report_type == "Y":
            # Get the last year's data
            end = first + dateutil.relativedelta.relativedelta(days=-1)
            start = first + dateutil.relativedelta.relativedelta(years=-1)


        elif report_type == "M":
            # Get the last month's data
            end = first + dateutil.relativedelta.relativedelta(days=-1)
            start = first + dateutil.relativedelta.relativedelta(months=-1)


        elif report_type == "W":
            start = first
            end = date

        dates.append([start, end])

    return dates


def process_reports(report_type, report_CA, report_US, file_title):
    """
    -------------------------------------------------------
    Processes two reports into one final report
    Use: new_file = process_reports(report_type, report_CA, report_US, file_title)
    -------------------------------------------------------
    Parameters:
        report_type - "Yearly", "Monthly", "MTD" (String)
        report_CA - Path to CA csv (str)
        report_US - Path to US csv (str)
        file_title - Title of final report generated (str)
    Returns:
       new_file - Path to newly generated report (str)
    ------------------------------------------------------
    """
    # combine report CSVs into one CSV
    all_data = pd.concat([report_CA, report_US], ignore_index=True)
    # get template
    template = pd.read_csv(os.path.join(".", "Templates", f"{report_type} Template.csv"))
    # zero out all template values except in columns 'Account ID - 18' , 'Compulsory' (all non numeric columns)
    keep_columns = ['Account ID - 18', 'Compulsory']
    for col in template.columns:
        if col not in keep_columns:
            template[col].values[:] = 0

    # in case any values in the template are missing (most likely Compulsary)
    for col in keep_columns:
        template[col] = template[col].fillna(0)

    # Fill in template with information
    template = create_upload(all_data, template)
    # Export updated data frame to csv
    new_file = os.path.join(".", "Reports", file_title)

    # Convert data to csv
    template.to_csv(str(new_file), index=False)
    print(f"The {report_type} Report has been successfully created.")
    print()
    return new_file


def get_template(sf, sf_url, sf_export_query, report_type, id):
    """
    -------------------------------------------------------
    Retrieve a template from SalesForce
    Use: get_template(sf, report_type, id)
    -------------------------------------------------------
    Parameters:
       sf - signed in Salesforce (Salesforce)
       report_type - "Yearly", "Monthly", "MTD" (String)
       id - ID specific to the report_type
    ------------------------------------------------------
    """

    SF_URL = "https://motoinsight.my.salesforce.com/"
    SF_EXPORT_QUERY = "?isdtp=p1&export=1&enc=UTF-8&xf=csv"
    TEMPLATES_DIR = os.path.join('.', "Templates")

    print(f"Getting {report_type} Template...")
    sf_report_url = sf_url + id + sf_export_query
    response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sf.session_id})
    report = response.content.decode('utf-8')
    df = pd.read_csv(StringIO(report))
    df.to_csv(os.path.join(TEMPLATES_DIR, f"{report_type} Template.csv"), index=False)
