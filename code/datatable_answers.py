# how many data sets per format per agency
# how much data (size) on data.gov

from datatable_parser import DB_FILENAME
import sqlite3

conn = sqlite3.connect(DB_FILENAME)
sql = conn.cursor()

for format in ['xml', 'csv', 'kml', 'shape']:
    sql.execute("select %s from datasets" % (format + 'size'))
    total = 0.0
    results = sql.fetchall()
    for size, in results:
        if not size: continue 
        size = size.lower()
        try:
            if 'kb' in size:
                total += float(size.replace('kb', '').strip())
            elif 'mb' in size:
                total += float(size.replace('mb', '').strip()) * 1024
        except Exception, e:
            pass
            #print format, size 
    count = len([row for row, in results if row])
    print "%10s: %4d files, %g MB" % (format, count, (total / 1024.0))

print "\n**************************************************\n"
         
sql.execute("select agency, count(xmlsize), count(csvsize), count(kmlsize), count(shapesize), count(otherurl), count(*) from datasets group by agency")
print "%s  %s %s %s %s %s %s" % ('agency', 'xml', 'csv', 'kml', 'shape', 'other', 'total')
for row in sql.fetchall():
    print "%6s: %3d %3d %3d %3d %3d %3d" % tuple(row)

