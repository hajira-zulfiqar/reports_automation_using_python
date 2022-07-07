import pandas as pd
import mysql.connector as connection
import yagmail
import os
from datetime import *

#connecting to the database
jugnudb = connection.connect(host="##########",
         database = '##########',
         user="##########",
         passwd="##########",
         use_pure=True)

sales_data_query = "SELECT \
                    sls.DeliveryDate AS InvoiceDate, \
                    WEEK(sls.DeliveryDate) AS InvoiceWeek, \
                    sls.`TownName` AS Location, \
                    sls.`StoreCode` AS CustomerNumber, \
                    sls.`StoreName` AS CustomerName, \
                    sls.`ChannelName` AS Channel, \
                    sls.`InvoiceNumber` AS InvoiceNumber, \
                    sls.`SKUCode` AS ItemCode, \
                    sls.`SKUDescription` AS ItemDescription, \
                    sls.`ManufacturerName` AS BusinessLineId, \
                    sls.`UnitsInCarton` AS UnitsPerCase, \
                    SUM(s.SKUWeight) AS Grammage, \
                    sls.CategoryName AS Category, \
                    oi.ItemPrice AS UnitSellingPriceWithoutGST, \
                    oi.TotalBillUnitDiscount + oi.CouponDiscount AS Discount, \
                    sls.OrderedQty AS OrderedQtyUnits, \
                    sls.DeliveredQty AS SoldQtyUnits, \
                    sls.`OrderedQTY`/sls.UnitsInCarton AS OrderedCases, \
                    sls.DeliveredQty / sls.UnitsInCarton AS DeliveredCases, \
                    (sls.OrderedValueIncTax) AS OrderedAmount, \
                    (sls.DeliveredValueIncTax) AS DeliveredAmount \
                    FROM \
                    sale_loss_summary sls \
                    INNER JOIN orders_items oi \
                    ON sls.OrderId = oi.OrderId \
                    LEFT JOIN skus s \
                    ON sls.SKUId = s.SKUId \
                    AND oi.SKUId = s.SKUId \
                    WHERE sls.DeliveryDate >= '2022-02-01' \
                    AND sls.`OrderStatus` NOT IN (6,10) \
                    AND sls.`ManufacturerId` IN (129,130) \
                    GROUP BY WEEK(sls.`DeliveryDate`), sls.OrderNumber, sls.CategoryId, sls.SKUCode, sls.DistributorId"

#removing the already existing price difference file
filepath_pd = "kellogs_and_pringles_sales_data.xlsx"

if os.path.exists(filepath_pd):
    os.remove(filepath_pd)
else:
    print("Sales data does not exist in the directory. Processing request now.")

#extracting  data for bulk deals
sales_data = pd.read_sql(sales_data_query, jugnudb)

#exporting excel files
sales_data.to_excel("kellogs_and_pringles_sales_data.xlsx", index = False)

#specifying the email details
recipient_email = ['##########']

#composing message for email subject and body
sales_data_subject = 'Sales Data - Kellogs & Pringles'
sales_data_body = 'Sales Data for Kellogs & Pringles from February 1, 2022 till Date '

#initializing server connection to send email
yag = yagmail.SMTP(user='##########', password='##########')

#sending out email for Price Difference Data
yag.send(
    to = recipient_email,
    subject = sales_data_subject,
    contents = sales_data_body,
    attachments = 'kellogs_and_pringles_sales_data.xlsx'
)