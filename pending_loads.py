import mysql.connector as connection
import yagmail
from sqlalchemy import null

#Connecting to DB
jugnudb = connection.connect(host="##########",
database = '##########',
user="##########",
passwd="##########",
use_pure=True)

cursor = jugnudb.cursor()

#Query for pending loads

pending_loads = "SELECT \
                I.`DeliveryDate`, \
                COUNT(DISTINCT(I.`LoadFormCode`)) AS NumberofLoads,\
                FORMAT(SUM(I.`InvoiceTotalWithTax`),'C') AS LoadformValue,\
                I.`RiderId`\
                FROM \
                invoices AS I\
                INNER JOIN load_form AS LF\
                ON I.`LoadFormCode` = LF.`LoadFormCode`\
                WHERE I.`DeliveryDate` = DATE_SUB(CURDATE(),INTERVAL 1 DAY)\
                AND I.`Status` = 1\
                AND LF.`CashSattlement`= 0\
                AND I.`DistributorId` IN (82,86)\
                GROUP BY I.`DeliveryDate`"

cursor.execute(pending_loads)
rows = cursor.fetchall()

#Print Output of Query
# print(len(rows))

if len(rows) == 0:
    msg = 'There are no pending loads today.'
else:
    msg = "The following are the pending loads in the system currently" + str(rows)
    

#initializing server connection to send email
yag = yagmail.SMTP(user='##########', password='##########')

#sending out email for Price Difference Data
yag.send(
    to = ['##########'],
    subject = 'Pending Loads for Today',
    contents = msg
)