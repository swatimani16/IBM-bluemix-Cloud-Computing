from flask import Flask,render_template,request
import os
import ibm_db
from datetime import datetime
#import math
import numpy as np
from timezonefinder import TimezoneFinder

import pytz
app = Flask(__name__)
conn=ibm_db.connect("DATABASE=BLUDB;HOSTNAME=;PORT=50000;PROTOCOL=TCPIP;UID=kdj50223;PWD=h3sbrwp6^7zzqmf5;", "", "")
port =int(os.getenv("VCAP_APP_PORT"))

bucketURL=''
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/qg5')
def qg5():
   return render_template('qg5.html')

@app.route('/search_depth')
def search_depth():
   return render_template('search_depth.html')


@app.route('/search_m_d')
def search_m_d():
   return render_template('search_m_d.html')

@app.route('/search_night')
def search_night():
   return render_template('search_night.html')

@app.route('/select_cluster')
def select_cluster():
    return render_template('select_cluster.html')

@app.route('/search_quake',methods=['GET','POST'])
def search_quake():
    if conn:
        if request.method=='POST':
            name=request.form['name']
            print(name)
            query = 'SELECT COUNT "mag" FROM KDJ50223.EARTHQUAKE WHERE "mag" > \'' + name + '\''
            stmt = ibm_db.prepare(conn, query)
            ibm_db.execute(stmt)
            rows = []
            result = ibm_db.fetch_assoc(stmt)
            print(result)
            while result != False:
                rows.append(result.copy())
                result = ibm_db.fetch_assoc(stmt)
            print(rows)
    return render_template('table.html', data=rows)

@app.route('/search_d',methods=['GET','POST'])
def search_d():
    if conn:
        if request.method=='POST':
            lat=request.form['lat']
            lon=request.form['lon']
            dist=request.form['dist']
            query = 'SELECT COUNT "mag" FROM (select *,(((acos(sin((' + lat + '*3.14/180)) * sin(("latitude"*3.14/180))+cos((' + lat + '*3.14/180))*cos(("latitude"*3.14/180))*cos(((' + lon + ' - "longitude")*3.14/180))))*180/3.14)*60*1.1515*1.609344) as distance from KDJ50223.EARTHQUAKE ) where distance <= ' + dist + ''
            stmt = ibm_db.prepare(conn, query)
            ibm_db.execute(stmt)
            rows = []
            result = ibm_db.fetch_assoc(stmt)
            print(result)
            while result != False:
                rows.append(result.copy())
                result = ibm_db.fetch_assoc(stmt)
            print(rows)
            count=len(rows)
        return render_template('table.html', data=rows)

@app.route('/search_m',methods=['GET','POST'])
def search_m():
    if conn:
        if request.method=='POST':
            date1=request.form['date11']
            date2=request.form['date21']
            mag1=request.form['mag11']
            mag2=request.form['mag21']
            query = 'SELECT COUNT "mag" FROM KDJ50223.EARTHQUAKE where "mag" BETWEEN ' + mag1 + ' and ' + mag2 + ' AND TIMESTAMP(SUBSTR("time", 1, 10)) >=   \'' + date1 + '\' and TIMESTAMP(SUBSTR("time", 1, 10)) <=   \'' + date2 + '\''
            stmt = ibm_db.prepare(conn, query)
            ibm_db.execute(stmt)
            rows = []
            result = ibm_db.fetch_assoc(stmt)
            print(result)
            while result != False:
                rows.append(result.copy())
                result = ibm_db.fetch_assoc(stmt)
            print(rows)
            count=len(rows)
    return render_template('table.html', data=rows)

@app.route('/search_c',methods=['GET','POST'])
def search_c():
    rows = []
    l = []
    latitude = []
    longitude = []
    if conn:
        lat1=float(request.form['lat1'])
        lon1=float(request.form['lon1'])
        print(lat1,lon1)
        lat2=float(request.form['lat2'])
        lon2=float(request.form['lon2'])
        grid1=int(request.form['size1'])
        grid2=int(request.form['size2'])
        for longi in np.arange(lon1, lon2, grid2):
            temp_longi = longi + grid2
            if temp_longi>lon2:
                temp_longi=lon2
            for lat in np.arange(lat1, lat2, grid1):
                temp_lat = lat + grid1
                if temp_lat>lat2:
                    temp_lat=lat2
                query = 'select count(*) from KDJ50223.EARTHQUAKE where "longitude" between ' + str(longi) + ' and ' + str(
                    temp_longi) + ' and "latitude" between ' + str(lat) + ' and ' + str(temp_lat)+''
                stmt = ibm_db.prepare(conn, query)
                ibm_db.execute(stmt)
                result = ibm_db.fetch_assoc(stmt)
                #print(result)
                l.append(int(result['1']))
                latitude.append(temp_lat)

                #print(l)
                while result != False:
                    rows.append(result.copy())
                    result = ibm_db.fetch_assoc(stmt)
                #print ("temp_lat:",temp_lat)
            #print("temp_lon:",temp_longi)
                longitude.append(temp_longi)
        #print("list of temp lat:", latitude)
        print("list of temp longitude:",longitude)
        m = max(l)
        print('Max:', m)
        im=l.index(m)
        lm=latitude[l.index(m)]
        print(lm)
        lonm=longitude[l.index(m)]
        print(lonm)
        count=(len(rows))
    return render_template('table2.html', count=count, data=rows,max=m,imax=im,latm=lm,lonm=lonm)


@app.route('/night2', methods=['GET', 'POST'])
def night2():
    rows = []
    rows1 = []
    if request.method=='POST':

        #for a day
        #query = 'SELECT * FROM Earthquake WHERE "mag" BETWEEN ' + mag +' AND ' + request.form['mag1'] + ' AND "time" LIKE \'%' + request.form['date'] + '%\''

        query = 'SELECT "mag","latitude","longitude","time" FROM KDJ50223.EARTHQUAKE WHERE "mag" > ' + request.form['mag']

        stmt = ibm_db.prepare(conn, query)
        ibm_db.execute(stmt)
        result = ibm_db.fetch_assoc(stmt)
        timefield = []
        while result != False:
            t=[]
            temp = result["time"]
            latitude = float(result["latitude"])

            longitude = float(result["longitude"])
            #print(latitude, longitude)
            timeb = temp[11:19]
            actualtime = datetime.strptime(timeb, '%H:%M:%S')
            #print(actualtime)
            tf = TimezoneFinder()
            try:
                newtimezone = pytz.timezone(tf.timezone_at(lng=longitude, lat=latitude))

            except pytz.UnknownTimeZoneError:
                newtimezone=pytz.timezone("Greenwich")

            oldtimezone = pytz.timezone("Greenwich")

            mytimestamp = oldtimezone.localize(actualtime).astimezone(newtimezone)
            hours = int(mytimestamp.strftime('%H'))
            if (hours >= 21 and hours <= 23) or (hours >= 0 and hours <= 6):

                rows.append(result.copy())

            else:
                rows1.append(result.copy())

            result = ibm_db.fetch_assoc(stmt)
            print(result)
            #mytimestamp = mytimestamp.replace(tzinfo=None)
            count=(len(rows))
            count1=(len(rows1))
        return render_template('table1.html', count=len(rows), count1=len(rows1), mag=request.form['mag'])
    return render_template('table1.html', count=len(rows), count1=len(rows1))

if __name__=='__main__':
    app.run(host='0.0.0.0',port=port,debug=False)