import pandas as pd
import mysql.connector as connection
import yagmail
import os
from datetime import *



#connecting to the database
jugnudb = connection.connect(host="="##################",
         database = '="##################',
         user="="##################",
         passwd="="##################",
         use_pure=True)

#query for price difference data
price_difference_query = "SELECT DATE(por.`AddedOn`) \
                        AS GRNDate,\
                        por.DistributorId,\
                        por.SupplierLegalEntityName AS SupplierName,\
                        por.GRNNumber,\
                        por.OrderNumber,\
                        bum.BussinessUnitName,\
                        sc.CategoryName,\
                        ss.SubCategoryName,\
                        pori.`SKUId`,\
                        s.SKUCode,\
                        s.SKUDescription,\
                        pori.TotalUnits AS ReceivedUnits,\
                        pori.`OrderedPriceET` AS OrderedPriceBeforeTax,\
                        pori.`ReceivedPriceETWithDiscount` AS ReceivedPriceBeforeTax,\
                        pori.`PriceDifferenceET` AS PriceDifferencePerUnit,\
                        pori.`TotalPriceDifferenceET` AS TotalPriceDifferenceET,\
                        pori.`ReceivedPriceETWithDiscount`,\
                        pori.`OrderedAdvanceTax` AS AdvancedTax,\
                        pori.`ReceivedTax` AS SalesTax,\
                        pori.`OrderedAdvanceTax` + pori.ReceivedTax AS PaymentPrice\
                        FROM\
                        primary_order_receiving_items pori\
                        LEFT JOIN primary_order_receiving por\
                        ON pori.PrimaryOrderReceivingId = por.PrimaryOrderReceivingId\
                        LEFT JOIN skus s\
                        ON pori.SKUId = s.SKUId\
                        INNER JOIN sku_categories sc\
                        ON s.CategoryId = sc.CategoryId\
                        INNER JOIN bussiness_unit_managment bum\
                        ON s.BussinessUnitId = bum.BussinessUnitId\
                        INNER JOIN sku_subcategories ss\
                        ON s.SubCategoryId = ss.SubCategoryId\
                        WHERE por.DistributorId IN (82,83,86)\
                        AND ROUND(pori.`TotalPriceDifferenceET`, 3) != 0 \
                        AND ROUND(pori.`PriceDifferenceET`, 3) != 0 \
                        AND DATE(por.AddedOn) >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)"

#query for pending finance check data
pending_finance_check_query = "SELECT \
                        DATE(por.`AddedOn`) AS GRNDate, \
                        po.`DistributorId`, \
                        por.SupplierLegalEntityName AS SupplierName, \
                        po.`OrderNumber` AS PONumber, \
                        por.`GRNNumber`, \
                        m.ManufacturerName, \
                        s.SKUCode, \
                        s.SKUDescription, \
                        pori.`TotalUnits`, \
                        pori.`OrderedPriceET` AS UnitPriceET, \
                        pori.`TotalUnits` * pori.OrderedPriceET AS TotalValueET \
                        FROM \
                        primary_order po \
                        INNER JOIN primary_order_receiving por \
                        ON po.`PrimaryOrderId` = por.`PrimaryOrderId` \
                        INNER JOIN primary_order_receiving_items pori \
                        ON por.`PrimaryOrderReceivingId` = pori.`PrimaryOrderReceivingId` \
                        INNER JOIN skus s \
                        ON pori.SKUId = s.SKUId \
                        INNER JOIN manufacturers m \
                        ON s.ManufacturerId = m.ManufacturerId \
                        WHERE por.`Status` = 0 \
                        AND po.DistributorId IN (82,83,86) \
                        AND DATE(por.AddedOn) >= '2022-01-01'"

#removing the already existing price difference file
filepath_pd = "price_differece.xlsx"

if os.path.exists(filepath_pd):
    os.remove(filepath_pd)
else:
    print("Price Difference data does not exist in the directory. Processing request now.")

#removing the already existing pending finance check file
filepath_pfc = "pending_finance_check.xlsx"

if os.path.exists(filepath_pfc):
    os.remove(filepath_pfc)
else:
    print("Pending Finance data does not exist in the directory. Processing request now.")


#extracting and manipulating data for price difference and pending finance check
price_difference_data = pd.read_sql(price_difference_query, jugnudb)
pending_finance_check_data = pd.read_sql(pending_finance_check_query, jugnudb)

#converting segment from O&I to Office and Industry
price_difference_data['DistributorId'].loc[(price_difference_data['DistributorId'] == 82)] = "##################"
price_difference_data['DistributorId'].loc[(price_difference_data['DistributorId'] == 86)] = "##################"
price_difference_data['DistributorId'].loc[(price_difference_data['DistributorId'] == 83)] = "################"

price_difference_data.rename(columns={"DistributorId":"DistributorName"}, inplace=True)

print(price_difference_data.head())

pending_finance_check_data['DistributorId'].loc[(pending_finance_check_data['DistributorId'] == 82)] = "##########"
pending_finance_check_data['DistributorId'].loc[(pending_finance_check_data['DistributorId'] == 86)] = "##########"
pending_finance_check_data['DistributorId'].loc[(pending_finance_check_data['DistributorId'] == 83)] = "##########"

pending_finance_check_data.rename(columns={"DistributorId":"DistributorName"}, inplace=True)

print(pending_finance_check_data.head())

#exporting excel files
price_difference_data.to_excel("price_differece.xlsx", index = False)
pending_finance_check_data.to_excel("pending_finance_check.xlsx", index = False)



# #specifying the email details
# recipient_email = ['##########']

# #composing message for email subject and body
# price_difference_subject = 'Price Difference Report' + ' ' + (datetime.now() - timedelta(1)).strftime('%d-%m-%Y')
# price_difference_body = 'Price Difference Raw Data for ' + (datetime.now() - timedelta(1)).strftime('%d-%m-%Y')

# pending_finance_check_subject = 'Pending Finance Check Report YTD'
# pending_finance_check_body = 'Pending Finance Check Raw Data YTD '

# #initializing server connection to send email
# yag = yagmail.SMTP(user='##########', password='##########')

# #sending out email for Price Difference Data
# yag.send(
#     to = recipient_email,
#     subject = price_difference_subject,
#     contents = price_difference_body,
#     attachments = 'price_differece.xlsx'
# )

# #sending out email for Finance Check Data
# yag.send(
#     to = recipient_email,
#     subject = pending_finance_check_subject,
#     contents = pending_finance_check_body,
#     attachments = 'pending_finance_check.xlsx'
# )