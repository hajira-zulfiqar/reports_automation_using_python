from cgi import test
from multiprocessing.sharedctypes import Value
from statistics import mean
from typing import final
from numpy import sort
import pandas as pd
import mysql.connector as connection
import yagmail
import os
from datetime import *

# os.chdir("enter file path here")
# print(os.getcwd())

# # connecting to the database
# jugnudb = connection.connect(host="##########",
#          database = '##########',
#          user="##########",
#          passwd="##########",
#          use_pure=True)

# #one stop shop
# one_stop_query = "SELECT MONTHNAME(sls.`DeliveryDate`), \
# sls.`DistributorId`, \
# sls.CategoryId, \
# sls.`Manufacturerid`, \
# SUM(sls.`DeliveredValueIncTax`) AS GMV, \
# SUM(sls.`OrderedValueIncTax`) AS OrderedValue, \
# SUM(sls.`OrderedValueIncTax`) /COUNT(DISTINCT(sls.ordernumber)) AS AOV, \
# COUNT(DISTINCT(sls.orderid))/COUNT(DISTINCT(sls.storecode)) AS PF, \
# COUNT(DISTINCT(sls.storecode)) AS StoreCount, \
# COUNT(DISTINCT(sls.ordernumber)) AS OrderCount, \
# SUM(sls.`DeliveredClaimableDiscountET`) AS ClaimableDiscount, \
# SUM(sls.`DeliveredNonClaimableDiscountET`) AS NonClaimableDiscount, \
# SUM(sls.DeliveredValueExcTaxBeforeDiscount-DeliveredNonClaimableDiscountET-DeliveredClaimableDiscountET-DeliveredCouponDiscount) AS NetSelling, \
# SUM(sls.`WUnitPriceET` * sls.`DeliveredQTY`) AS TotalBuying, \
# sls1.YesterdaysOrderedValue, \
# sls2.YesterdaysDeliveredValue, \
# st1.GMVTarget, \
# desc1.ClosingInventoryValue, \
# desc1.ClosingInventoryUnits \
# FROM \
# sale_loss_summary sls \
# LEFT JOIN ( \
# SELECT \
# (sls.orderdate), \
# sls.categoryid, \
# sls.distributorid, \
# sls.manufacturerid, \
# SUM(sls.orderedvalueinctax) AS YesterdaysOrderedValue \
# FROM \
# sale_loss_summary AS sls \
# WHERE MONTH(sls.orderdate)>=2 \
# AND YEAR(sls.orderdate)=2022 \
# AND DAY(sls.orderdate)=(DAY(CURDATE())-1) \
# AND sls.orderstatus NOT IN (6,10) \
# AND sls.`DistributorId` IN (82,83,86,87,88,90) \
# GROUP BY sls.orderdate,sls.`DistributorId`,sls.`CategoryId`,sls.`ManufacturerId`) AS sls1 \
# ON sls.categoryid=sls1.categoryid \
# AND sls.manufacturerid=sls1.manufacturerid \
# AND sls.distributorid=sls1.distributorid \
# AND MONTH(sls.deliverydate)=MONTH(sls1.orderdate) \
# LEFT JOIN ( \
# SELECT \
# (sls.deliverydate), \
# sls.categoryid, \
# sls.distributorid, \
# sls.manufacturerid, \
# SUM(sls.deliveredvalueinctax) AS YesterdaysDeliveredValue \
# FROM \
# sale_loss_summary AS sls \
# WHERE MONTH(sls.deliverydate)>=2 \
# AND YEAR(sls.deliverydate)=2022 \
# AND DAY(sls.deliverydate)=(DAY(CURDATE())-1) \
# AND sls.orderstatus NOT IN (6,10) \
# AND sls.`DistributorId` IN (82,83,86,87,88,90) \
# GROUP BY sls.deliverydate,sls.`DistributorId`,sls.`CategoryId`,sls.`ManufacturerId`) AS sls2 \
# ON sls.categoryid=sls2.categoryid \
# AND sls.manufacturerid=sls2.manufacturerid \
# AND sls.distributorid=sls2.distributorid \
# AND MONTH(sls.deliverydate)=MONTH(sls2.deliverydate) \
# LEFT JOIN ( \
# SELECT \
# st.categoryid, \
# st.distributorid, \
# st.manufacturerid, \
# st.Month, \
# SUM(st.GMVTarget) AS GMVTarget \
# FROM \
# supplier_targets AS st \
# WHERE st.month>1 \
# AND st.Year = 2022 \
# GROUP BY st.`DistributorId`,st.`CategoryId`,st.`ManufacturerId`, st.Month) AS st1 \
# ON st1.distributorid=sls.distributorid \
# AND st1.categoryid=sls.categoryid \
# AND st1.manufacturerid=sls.manufacturerid \
# AND MONTH(sls.DeliveryDate) = st1.Month \
# LEFT JOIN ( \
# SELECT \
# (des.endstockdate), \
# s.`CategoryId`, \
# s.`ManufacturerId`, \
# des.`DistributorId`, \
# sum(des.ClosingInventory) as ClosingInventoryUnits, \
# SUM(des.WUnitPriceIT*des.closinginventory) AS ClosingInventoryValue \
# FROM daily_end_stock des \
# INNER JOIN skus AS s \
# ON des.`SKUId`=s.`SKUId` \
# WHERE MONTH(des.`EndStockDate`) >=2 \
# AND YEAR(des.endstockdate)=2022 \
# AND DAY(des.endstockdate)=(DAY(CURDATE())-1) \
# AND des.`DistributorId` IN (82,83,86,87,88,90) \
# AND des.`DistributorLocationId` IN (166,170,179,183,185,190) \
# GROUP BY des.endstockdate,des.`DistributorId`,s.`CategoryId`,s.`ManufacturerId`) AS desc1 \
# ON desc1.categoryid=sls.categoryid \
# AND desc1.manufacturerid=sls.manufacturerid \
# AND desc1.distributorid=sls.distributorid \
# AND MONTH(sls.deliverydate)=MONTH(desc1.endstockdate) \
# WHERE sls.DeliveryDate >= '2022-02-01' \
# AND sls.`OrderStatus` NOT IN (6,10) \
# AND sls.`DistributorId` IN (82,83,86,87,88,90) \
# GROUP BY MONTH(sls.`DeliveryDate`),sls.`DistributorId`,sls.`CategoryId`,sls.`ManufacturerId`"



# #extracting  data for bulk deals
# one_stop_shop = pd.read_sql(one_stop_query, jugnudb)

# #exporting excel files
# one_stop_shop.to_excel("one_stop_shop.xlsx", index = False)

#importing excel
query_data = pd.read_excel("data_for_pivot.xlsx")

#transformed_data = query_data >> query_data.sort_values("DeliveryMonth") >> group_by(f.CatgoryId, f.ManufacturerId, f.DistributorId) >> mutate(avg_gmv = (query_data['GMV'].shift(1) + query_data['GMV'].shift(2) + query_data['GMV'].shift(3)/3)

#making the data pivot ready
data_for_pivot = query_data.melt(id_vars = query_data.iloc[:, 0:4],
                                var_name="Measure",
                                value_name="Numerics")


data_for_pivot['Measure'] = data_for_pivot['Measure'].astype('category')

# data_for_pivot.to_excel('pivot_data.xlsx')

#print(data_for_pivot)
# data_for_pivot['Measure'].astype('category')
data_for_pivot['Measure'].cat.reorder_categories(['GMVTarget', 
'GMV', 'OrderedValue' ,'YesterdaysOrderedValue', 'YesterdaysDeliveredValue', 'StoreCount', 
'AOV', 'PF', 'OrderCount', 'NonClaimableDiscount', 
'ClaimableDiscount', 'ClosingInventoryValue', 'GrossMargin'], inplace=True)


#creating the pivot table
final_view = pd.pivot_table(data_for_pivot,
                            index=['DistributorId', 'CategoryName', 'ManufacturerName'],
                            values=['Numerics'],
                            columns=['Measure', 'DeliveryMonth'])


# final_view = final_view['percent_of_total'] = round((final_view.Values) / final_view.groupby(level = 0).Values.transform(sum)*100, 0).astype(int)

#removing the already existing price difference file
filepath_pd = "final_view_test.xlsx"

if os.path.exists(filepath_pd):
    print("Existing Pivot Data file is being removed.")
    os.remove(filepath_pd)
else:
    print("Pivot Data does not exist in the directory. Processing request now.")

#print(final_view)
final_view.to_excel('final_view_test.xlsx')

#enter the code to automatically send the pivot data via email