import pandas as pd
import numpy as np
import mysql.connector as connection
import yagmail
import os
from datetime import *

#connecting to the database
jugnudb = connection.connect(host="#########",
         database = '###########',
         user="#################",
         passwd="#################3",
         use_pure=True)

#query for active retailers data
active_retailers_query = "SELECT sls.TownName, \
sls.StoreCode, \
sls.`RetailerCode`, \
sls.`RetailerId`, \
sls.`ContactNumber`, \
sls.`StoreName`, \
sls.`PickListZoneLabel` AS ZoneName, \
CONCAT(r.`FirstName`, ' ', r.`LastName`) AS Retailer_Name, \
sls1.AOV, \
sls1.PurchaseFrequency, \
sls1.last_order AS LastOrderDate \
FROM \
sale_loss_summary sls \
INNER JOIN ( SELECT sls.`RetailerCode`, \
SUM(sls.OrderedValueIncTax)/COUNT(DISTINCT(sls.OrderNumber)) AS AOV, \
COUNT(DISTINCT(sls.OrderNumber))/COUNT(DISTINCT(EXTRACT(YEAR_MONTH FROM sls.`DeliveryDate`))) AS PurchaseFrequency, \
MAX(OrderDate) AS last_order \
FROM \
sale_loss_summary sls \
WHERE sls.DeliveryDate >= '2021-01-01' \
AND sls.OrderStatus NOT IN (6,10,11) \
AND sls.DistributorId IN (82,83,86,88,90) \
GROUP BY sls.RetailerCode \
) AS sls1 \
ON sls.`RetailerCode` = sls1.RetailerCode \
INNER JOIN retailer r \
ON sls.`RetailerId` = r.`RetailerId` \
WHERE MONTH(sls.DeliveryDate) = MONTH(CURDATE()) \
AND sls.`OrderStatus` NOT IN (6,10,11) \
AND sls.`DistributorId` IN (83,88,90) \
GROUP BY sls.StoreCode \
HAVING MIN(sls.DeliveryDate) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)"

#specifying working directory
# os.chdir("C:\python")

#extracting data for active retailers
active_retailers_yesterday = pd.read_sql(active_retailers_query, jugnudb)

#removing the already existing price difference file
filepath_pd = "active_retailers_yesterday.xlsx"

if os.path.exists(filepath_pd):
    print('Removing the existing Active Retailers file from the directory.')
    os.remove(filepath_pd)
    print('File Removed. Processing request now.')
else:
    print("Active Retailers File does not exist in the directory. Processing request now.")

#exporting excel files
active_retailers_yesterday.to_excel("active_retailers_yesterday.xlsx", index = False)

#specifying the email details
recipient_email = ['#################']

#composing message for email subject and body
active_retailers_data_subject = 'jugnu | default | send survey invites | ' + datetime.strftime(datetime. today()  - timedelta(days=1), '%Y-%m-%d')
active_retailers_data_body = 'Active Retailers Data for ' + datetime.strftime(datetime. today()  - timedelta(days=1), '%Y-%m-%d')

#initializing server connection to send email
yag = yagmail.SMTP(user='#############', password='###############')

#sending out email for Price Difference Data
yag.send(
    to = recipient_email,
    subject = active_retailers_data_subject,
    contents = active_retailers_data_body,
    attachments = ['active_retailers_yesterday.xlsx']
)