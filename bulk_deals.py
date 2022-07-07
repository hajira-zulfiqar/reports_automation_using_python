import pandas as pd
import mysql.connector as connection
import yagmail
import os
from datetime import *

#connecting to the database
jugnudb = connection.connect(host="##################",
         database = '="##################',
         user="="##################",
         passwd="="##################",
         use_pure=True)

#query for bulk deals
bulk_deals_query = "SELECT\
                    u.UserName,\
                    o.OrderDate,\
                    o.`OrderNumber`,\
                    o.`StoreId`,\
                    sls.DeliveredValueIncTax,\
                    o.DeliveryDate,\
                    sls.SKUId,\
                    sls.SKUDescription,\
                    sls.DeliveredValueExcTaxBeforeDiscount - sls.DeliveredNonClaimableDiscountET - \
                    sls.DeliveredClaimableDiscountET - sls.DeliveredCouponDiscount AS NetSelling,\
                    sls.WUnitPriceET * sls.DeliveredQty AS Cost,\
                    sls.DeliveredValueExcTaxBeforeDiscount,\
                    sls.DeliveredNonClaimableDiscountET,\
                    sls.DeliveredClaimableDiscountET,\
                    sls.DeliveredCouponDiscount,\
                    ta.TemporaryAdjustmentValue,\
                    ((sls.DeliveredValueExcTaxBeforeDiscount - \
                    sls.DeliveredNonClaimableDiscountET - \
                    sls.DeliveredClaimableDiscountET - \
                    sls.DeliveredCouponDiscount) - \
                    (sls.WUnitPriceET * sls.DeliveredQty) +\
                    sls.DeliveredClaimableDiscountET)/\
                    (sls.DeliveredValueExcTaxBeforeDiscount - \
                    sls.DeliveredNonClaimableDiscountET - \
                    sls.DeliveredClaimableDiscountET - \
                    sls.DeliveredCouponDiscount) AS TM \
                    FROM \
                    orders o \
                    INNER JOIN users u \
                    ON o.`OrderAddedBy` = u.`UserId` \
                    LEFT JOIN sale_loss_summary sls \
                    ON o.OrderId = sls.OrderId \
                    LEFT JOIN temporary_adjustment ta \
                    ON sls.SKUId = ta.SKUId \
                    AND sls.OrderId = ta.OrderId \
                    AND sls.DistributorId = ta.DistributorId \
                    WHERE o.`DeliveryDate` >= '2022-02-01' \
                    AND sls.OrderedQty > 0 \
                    AND sls.DistributorId IN (82,83,86) \
                    AND o.OrderPlacedFrom = 2 \
                    AND sls.OrderStatus NOT IN (6,10) \
                    GROUP BY sls.OrderNumber, sls.SKUId"

# #removing the already existing price difference file
# filepath_pd = "bulk_deals.xlsx"

# if os.path.exists(filepath_pd):
#     os.remove(filepath_pd)
# else:
#     print("Bulk Deals data does not exist in the directory. Processing request now.")

# #extracting  data for bulk deals
# bulk_deals_data = pd.read_sql(bulk_deals_query, jugnudb)

# #exporting excel files
# bulk_deals_data.to_excel("bulk_deals.xlsx", index = False)

# #specifying the email details
# recipient_email = ['="##################',
#                 '="##################',
#                 '="##################']

# #composing message for email subject and body
# bulk_deals_subject = 'Bulk Deals Data'
# bulk_deals_body = 'Bulk Deals Data from February 1, 2022 till Date '

# #initializing server connection to send email
# yag = yagmail.SMTP(user='a="##################', password='="##################')

# #sending out email for Price Difference Data
# yag.send(
#     to = recipient_email,
#     subject = bulk_deals_subject,
#     contents = bulk_deals_body,
#     attachments = 'bulk_deals.xlsx'
# )