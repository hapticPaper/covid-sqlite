SUMMARY_CSV = """SELECT * FROM
                (SELECT combinedKey,lat, lng, confirmed, deaths, recovered,
                    d.lastUpdate,
                    lag(confirmed) OVER (order by combinedkey, d.lastUpdate) yesterdayConfirmed,
                    round(100*cast((confirmed-(lag(confirmed) OVER (partition by combinedkey order by combinedkey, d.lastUpdate))) as decimal)/(nullif(lag(confirmed) OVER (partition by combinedkey order by combinedkey, d.lastUpdate),0)),2) increaseRate,
                    confirmed - (lag(confirmed) OVER (partition by combinedkey order by combinedkey, d.lastUpdate)) newCases,
                    recovered-(lag(recovered) OVER (partition by combinedkey order by d.lastUpdate)) newRecoveries,
                    deaths - (lag(deaths) OVER (partition by combinedkey order by d.lastUpdate)) newDeaths
                        FROM (SELECT distinct * from daily
                                --where combinedkey='Unassigned, Tennessee, US'
                            ) d
        ) r
    where (newCases is not null or newRecoveries is not null  or newDeaths is not null)
    AND increaseRate<1000
    order by newCases desc
        """



UPDATE_KEY = """
UPDATE daily SET combinedkey = replace(replace(case
			when admin2 is not null then admin2|| ', '|| provincestate||', '|| countryregion
			when provincestate is not null then provincestate||', '|| countryregion
	  		else countryregion
	  end, 'Unassigned, ', ''), 'unassigned','')
  WHERE combinedkey is null;
  """

NEW_PLACES = """SELECT distinct daily.combinedKey, k.lat, k.lng
FROM daily
LEFT JOIN keycoords k on daily.combinedKey = k.combinedKey
WHERE (k.lat is null or k.lng is null) AND not (lower(daily.combinedKey) like '%unknown')
"""

LAST_UPDATE = """SELECT max(date(lastupdate)) lu FROM daily"""

DATES=f"""SELECT distinct date(lastupdate) date from  us_daily where date(lastUpdate) is not null order by 1"""


def INSERT(fields, values):
    return f"""INSERT INTO daily {fields}
               VALUES {str(values)}
    """

def COPY(table, columns):
    return  f"""COPY {table} {columns}
FROM STDIN
WITH 
(FORMAT CSV,
DELIMITER ',',
HEADER TRUE)
"""

def LOADLOCAL(table, columns, file):
    return  f"""COPY {table} 
FROM '{file}'
WITH 
(FORMAT CSV,
DELIMITER ',',
HEADER TRUE)
"""

def DAILY_UPDATE(datey): 
    return f"""SELECT round(cast(k.lat as numeric),2) lat, round(cast(k.lng as numeric),2) lng, max(lastupdate) lastupdate, max(confirmed) confirmed, max(deaths) deaths 
    from daily d left join keycoords k on k.combinedkey=d.combinedkey  where  date(lastupdate)='{datey}' group by  1, 2, k.combinedkey
    """


def UPDATE_KEY_GEO(location, lat, lng): 
    return (f"UPDATE daily SET lng={lng}, lat={lat} WHERE combinedkey='{location}';",
           f"UPDATE keycoords SET lng={lng}, lat={lat} WHERE combinedkey='{location}';",
           f"Insert into keycoords (lng, lat, combinedKey) Values ('{lng}','{lat}','{location}')")


