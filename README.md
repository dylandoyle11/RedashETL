# Redash Reports v2

This script is used to generate regular reports on sales metrics. This 
reports is generated using Redash and SQL queries which are then 
uploaded to Salesforce via Python Automation. 

The program works differently for different platforms. For 'Darwin' and 
'Windows' platforms the user has to manually select which reports to 
generate. However, for 'Linux' platforms, all reports are generated. 
This is purely done to run this script on a server without needing it to 
enter inputs everytime. 

The program is divided into following 4 steps:
- Step 1: Get user input for what kind of report to create
- Step 2: Get Templates from Salesforce
- Step 3: Generate and Download Redash report online
- Step 4: Use SlackBot to send messages and attachments to channel



## Author(s)

- [Dylan Doyle](https://github.com/ddoyle-moto)



## Clone

Clone this repository to your local machine using the following command:

`git clone https://github.com/motoinsight-data-ops/Redash_Reports_V2.git`
