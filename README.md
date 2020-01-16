```python
def getXYIntfromDatabase():

  # USE healthdb;
  # DROP TABLE IF EXISTS tblRawData;
  # CREATE TABLE tblRawData (orderId INT, val INT);
  # INSERT INTO tblRawData (orderId, val) VALUES (1, 1), (2, 2), (3, NULL), (4, NULL), (5, 5);

  import scipy.interpolate
  import mysql.connector
  from mysql.connector import Error
  from mysql.connector import errorcode

  try:
    connection = mysql.connector.connect(host="localhost", user="root", passwd="london12", database="healthdb")

    cursor = connection.cursor()

    sql = """ SELECT * FROM tblRawData ORDER BY OrderId """

    cursor.execute(sql)
    result = cursor.fetchall()

    # print(result)
    return result

  except mysql.connector.Error as error:
    print("Error: {}".format(error))

  finally:
    if (connection.is_connected()):
      cursor.close()
      connection.close()
      # print("MySQL connection is closed")


def getXYDatefromDatabase():

  # USE healthdb;
  # DROP TABLE IF EXISTS tblRawData;
  # CREATE TABLE tblRawData (orderId INT, val INT);
  # INSERT INTO tblRawData (orderId, val) VALUES (1, 1), (2, 2), (3, NULL), (4, NULL), (5, 5);

  import scipy.interpolate
  import mysql.connector
  from mysql.connector import Error
  from mysql.connector import errorcode

  try:
    connection = mysql.connector.connect(host="localhost", user="root", passwd="london12", database="healthdb")

    cursor = connection.cursor()

    sql = """ SELECT * FROM tblRawDataDate ORDER BY date """

    cursor.execute(sql)
    result = cursor.fetchall()

    # print(result)
    return result

  except mysql.connector.Error as error:
    print("Error: {}".format(error))

  finally:
    if (connection.is_connected()):
      cursor.close()
      connection.close()
      # print("MySQL connection is closed")

def getXYDatefromDatabase_healthData():

  import scipy.interpolate
  import mysql.connector
  from mysql.connector import Error
  from mysql.connector import errorcode

  try:
    connection = mysql.connector.connect(host="localhost", user="root", passwd="london12", database="healthdb")

    cursor = connection.cursor()

    sql = """ SELECT date, weight FROM tblTimeSeriesData WHERE date >= '2017-01-26' AND date <= '2020-01-06' ORDER BY date """

    cursor.execute(sql)
    result = cursor.fetchall()

    # print(result)
    return result

  except mysql.connector.Error as error:
    print("Error: {}".format(error))

  finally:
    if (connection.is_connected()):
      cursor.close()
      connection.close()
      # print("MySQL connection is closed")


def interpolateInts(XY):

  import scipy.interpolate

  listX = []
  listX1 = []
  listY = []
  listY1 = []

  for item in XY:
    listX1.append(item[0])
    if item[1] != None:
      listX.append(item[0])
      listY.append(item[1])

  y_interp = scipy.interpolate.interp1d(listX, listY)

  for item in listX1:
    listY1.append(float(y_interp(item)))

  # print(listX, listY, listX1, listY1)
  return(listX, listY, listX1, listY1)


def interpolateDates(XY):

  print("____________________________________")

  import scipy.interpolate
  from datetime import datetime

  listX = []
  listX0 = []
  listX1 = []
  listY = []
  listY1 = []

  for item in XY:
    listX0.append(item[0])
    listX1.append(int(item[0].strftime('%Y%m%d')))

    if item[1] != None:
      listX.append(int(item[0].strftime('%Y%m%d')))
      listY.append(item[1])

  y_interp = scipy.interpolate.interp1d(listX, listY)

  for item in listX1:
    listY1.append(float(y_interp(item)))

  # print(listX, listY, listX1, listY1)
  return(listX, listY, listX1, listY1, listX0)


def insertMultipleRecordDataToTable(X1, Y1):

  # USE healthdb;
  # DROP TABLE IF EXISTS tblRawDataInterpolated;
  # CREATE TABLE tblRawDataInterpolated (orderId INT, valInterpolated FLOAT);

  import mysql.connector
  from mysql.connector import Error
  from mysql.connector import errorcode

  try:
    connection = mysql.connector.connect(host="localhost", user="root", passwd="london12", database="healthdb")

    sql = """ INSERT INTO tblRawDataInterpolated (orderId, valInterpolated) VALUES (%s, %s) """
    merged_list = [(X1[i], Y1[i]) for i in range(0, len(X1))] 

    cursor = connection.cursor()
    cursor.executemany(sql, merged_list)
    connection.commit()
    print("Records Inserted: ", cursor.rowcount)


  except mysql.connector.Error as error:
    print("Failed to insert into MySQL table {}".format(error))

  finally:
    if (connection.is_connected()):
      cursor.close()
      connection.close()
      # print("MySQL connection is closed")


def insertMultipleRecordDateDataToTable(X1, Y1):

  # USE healthdb;
  # DROP TABLE IF EXISTS tblRawDataDateInterpolated;
  # CREATE TABLE tblRawDataDateInterpolated (date DATE, valInterpolated FLOAT);

  import mysql.connector
  from mysql.connector import Error
  from mysql.connector import errorcode

  try:
    connection = mysql.connector.connect(host="localhost", user="root", passwd="london12", database="healthdb")

    sql = """ INSERT INTO tblRawDataDateInterpolated (date, valInterpolated) VALUES (%s, %s) """
    merged_list = [(X1[i], Y1[i]) for i in range(0, len(X1))] 

    cursor = connection.cursor()
    cursor.executemany(sql, merged_list)
    connection.commit()
    print("Records Inserted: ", cursor.rowcount)

  except mysql.connector.Error as error:
    print("Failed to insert into MySQL table {}".format(error))

  finally:
    if (connection.is_connected()):
      cursor.close()
      connection.close()
      # print("MySQL connection is closed")




dbResults  = getXYDatefromDatabase_healthData()
X, Y, X1, Y1, X0 = interpolateDates(dbResults)
print (X0, Y1)
insertMultipleRecordDateDataToTable(X0, Y1)

print(dbResults)

exit() 

dbResults = getXYIntfromDatabase()
X, Y, X1, Y1 = interpolateInts(dbResults)
print (X1, Y1)
insertMultipleRecordDataToTable(X1, Y1)

print("__________________________________")

dbResults = getXYDatefromDatabase()
X, Y, X1, Y1, X0 = interpolateDates(dbResults)
print (X0, Y1)
insertMultipleRecordDateDataToTable(X0, Y1)



```
