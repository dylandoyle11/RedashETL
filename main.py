#!/usr/bin/python3
# EXAMPLE CHANGE
"""
------------------------------------------------------------------------
Redash Reports Handler
------------------------------------------------------------------------
Author(s): Dylan Doyle
File Name: main.py
Updated: 2021-08-19
------------------------------------------------------------------------
Notes:


The program is divided into following 4 steps:
- Step 1: Get user input for what kind of report to create
- Step 2: Get Templates from Salesforce
- Step 3: Generate and Download Redash report online, fill in Template with Redash data
- Step 4: Use SlackBot to send messages and attachments to channel
------------------------------------------------------------------------
"""

from functions import *
from io import StringIO
from simple_salesforce import Salesforce
import traceback
import csv
import numpy as np
import pandas as pd
import pathlib
import platform
import requests
import time
import sys
import json

try:
    sys.path.insert(1, os.path.join('..', 'SlackBot'))
    from SlackBot import *
except ImportError as e:
    print(e)
    raise('Cannot import SlackBot. Either it is not installed or the path "../SlackBot/SlackBot.py" does not point to it.')

DATE = date = datetime.date(datetime.now())
DOWNLOAD_DIR = os.path.join('.', "Exports")
TEMPLATES_DIR = os.path.join('.', "Templates")

# Salesforce API (for templates)
YEARLY_ID = "00O1I000006q0XDUAY"
MONTHLY_ID = "00O1I000006q0WtUAI"
MTD_ID = "00O1I000006q0VvUAI"
USERNAME = "ddoyle@motoinsight.com"
PASSWORD = "Mnidad1101#"
TOKEN = "un8DJcYRICWEQPjtJeSWuLOG4"
SF_URL = "https://motoinsight.my.salesforce.com/"
SF_EXPORT_QUERY = "?isdtp=p1&export=1&enc=UTF-8&xf=csv"

# Redash API (for reports)
API_KEY_CA = "TgpH26hjcg70BAphHJtA4GYmFa0fViVwu4tlDj4A"
API_KEY_US = "JeKQKZpKuJRGHNDUs8RhMF42FhDukoA4aqGeUf97"
HEADERS_CA = {'Authorization': 'Key TgpH26hjcg70BAphHJtA4GYmFa0fViVwu4tlDj4A'}
HEADERS_US = {'Authorization': 'Key JeKQKZpKuJRGHNDUs8RhMF42FhDukoA4aqGeUf97'}
JOB_URL_CA = "https://redash.motocommerce.ca/api/jobs/{}"
JOB_URL_US = "https://redash.motocommerce.com/api/jobs/{}"

QUERY_ID_CA = '377'
QUERY_ID_US = '150'

QUERY_ID_CA_BACKUP = '569'
QUERY_ID_US_BACKUP = '231'


URL_CA = "https://redash.motocommerce.ca"
URL_US = "https://redash.motocommerce.com"

# Columns in Final Report
# must be spelled exactly as Redash report export display
REPORT_COLUMNS = ['Dealer Salesforce ID', 'Compulsory', 'Total Leads',
                  'Dealer Created Leads', 'ADF Leads', 'Organic Leads', 'AutoTrader Leads',
                  'Cars Solds', 'Unique Users', 'Page Views', 'Sessions',
                  'Appointments', 'Trade-ins', 'Credit Apps', 'Deposits',
                  'Showroom Unique Visitors']

reports = ('Yearly', 'Monthly', 'MTD')
report_types = ('Y', 'M', 'W')

"""
=============================
MAIN
=============================
"""


def main():

    timer_start = time.time()
    intro()

    # Retrieve date ranges for each report
    dates = get_dates(report_types)

    # Sign into Salesforce
    sf = Salesforce(username=USERNAME, password=PASSWORD, security_token=TOKEN)
    # Get all 3 templates
    get_template(sf, SF_URL, SF_EXPORT_QUERY, "Yearly", YEARLY_ID)
    get_template(sf, SF_URL, SF_EXPORT_QUERY, "Monthly", MONTHLY_ID)
    get_template(sf, SF_URL, SF_EXPORT_QUERY, "MTD", MTD_ID)

    # Create list of created files and titles to be passed to Slackbot later
    files = []
    titles = []

    # For each report type requested, get data, process and format into final report
    for i, report in enumerate(reports):
        # Get correct report type for All Reports option

        report_type = report_types[i]

        # Extract respective date range
        start = dates[i][0]
        end = dates[i][1]

        # Get CA report
        report_CA, filename = get_report(DOWNLOAD_DIR, URL_CA, report_type, HEADERS_CA, QUERY_ID_CA, start, end, REPORT_COLUMNS)
        report_CA = format_report(report_type, report_CA, REPORT_COLUMNS, start, filename)
        print()

        # Get US report
        time.sleep(2)
        report_US, filename = get_report(DOWNLOAD_DIR, URL_US, report_type, HEADERS_US, QUERY_ID_US, start, end, REPORT_COLUMNS)
        report_US = format_report(report_type, report_US, REPORT_COLUMNS, start, filename)
        print()

        # Create final report name
        file_title = f"{DATE}_{report} Report ({start} - {end}).csv"
        # Process the CA and US reports into a single final report
        new_file = process_reports(report, report_CA, report_US, file_title)

        # Add file and name to lists to send to SlackBot later
        files.append(str(new_file))
        titles.append(file_title)


    timer_end = time.time()
    print(":::> Time Elapsed to get the reports: ", timer_end - timer_start)
    print()

    print("Sending files via Slackbot...")
    channels = ['redashdealerreports']
    messages = ['Your Redash reports are ready!']
    slack_send_message(channels, messages)
    slack_send_file(channels, files, titles)
    print("Files sent. Process Complete.")


if __name__ == '__main__':
    main()
