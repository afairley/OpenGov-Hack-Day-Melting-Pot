# datatable_parser.py
# purpose:
#          extract data from 
#          http://www.data.gov/catalog/raw/category/0/agency/0/filter//type/xckeo/sort//page/1/count/600
#          to put in a sqlite db that we can ask 
#          detailed questions quickly about what's available on data.gov
# done:   
#          I've taken the relevant table out of the html
#          and put it in data/datatable.html
#          The data in the html is in the format
#          col1 [link to details at data.gov, snippet descibing data] 
#          col2 [rating as an img, alt text has the rating]
#          col3 [Agency] 
#data     /col4 xml        [link to content if present]
#formats / col5 csv        [link to content if present]
#avail -<  col6 kml        [link to content if present]
#        \ col7 shapefile  [link to content if present]
#         \col8 other      [link to another page to parse for spreadsheets and other exotic file formats]
#         I've written here a hacky beautiful soup parser to pull the
#           rows out and iterate over them
#         I also wrote a hacky parser to get the data in the
#           first column out and put it in a labeled dictionary 
#         There's hacky error handling etc.
# todo:   
#         [ ] parse out the remaining 7 columns and make a nice structure with all the relavent goodies
#         [ ] some sqlalchemy to put the data in a table like:
#          name    : "Link Text From Column 1"
#          descr   : "snippet from   Column 1"
#          formats : bitvector of length 5 describing presence absence of xml,csv,kml,shapefile,other
#          xmllink : URL of xml content if present
#          csvlink  : URL of csv content if present
#          ...
#        [ ] fetch and parse the 'other' pages?
#        [ ] use urlib and beautiful soup to make this a proper scraper rather than wget and vim :D

FILENAME =  ("/home/vegas/Documents/Projects" + #replace this absolute path 
             "/hackday/opengov/"              + #replace this absolute path
             "data/datatable.html")             #relative path for datafile

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey
from BeautifulSoup import BeautifulSoup

metadata       = MetaData()
DB_FILENAME = ("/home/vegas/Documents/Projects"         + #replace this absolute path 
                        "/hackday/opengov/"                      + #replace this absolute path
                        "data/datatable.sqlite3")                  #relative path for datafile

engine = create_engine('sqlite:///' + DB_FILENAME)
metadata.bind = engine

dataset_table = Table('datasets',metadata,
    Column('id',Integer, primary_key=True),
    Column('relUrl',     String, nullable=True),     
    Column('relUrlName', String, nullable=True),  
    Column('descr',      String, nullable=True),      
    Column('agency',     String, nullable=True),     
    Column('xmlurl',     String, nullable=True),     
    Column('xmlsize',    String, nullable=True),    
    Column('csvurl',     String, nullable=True),     
    Column('csvsize',    String, nullable=True),    
    Column('kmlurl',     String, nullable=True),     
    Column('kmlsize',    String, nullable=True),    
    Column('shapeurl',   String, nullable=True),   
    Column('shapesize',  String, nullable=True),  
    Column('mapsurl',    String, nullable=True),   
    Column('otherurl',   String, nullable=True)
)   

metadata.create_all( engine )


def parseLinkAndTextFromCol1(col):
    col = col.contents[0]
    link       = col.findAll('a')[0]
    relUrl     = link.get('href')
    relUrlName = "".join(link.contents)
    
    text = col.findAll('span')[0]
    text = text.contents[0]
    return {
             'relUrl'     : relUrl,
             'relUrlName' : relUrlName,
             'descr'       : text
           }
def parseRatingFromCol2(col):
    return {'rating' :  col.contents[0].get('alt') }
def parseAgencyFromCol3(col):
    return {'agency' :  col.contents[0]}
def parseXMLInfoFromCol4(col):
    a = col.findAll('a')
    if len(a) > 0:
        a = a[0]
        return {
            'xmlurl'  : a.get('href'),
            'xmlsize' : a.contents[2].strip()
            }
    else:
        return {}
def parseCSVInfoFromCol5(col):
    a = col.findAll('a')
    if len(a) > 0:
        a = a[0]
        return {
            'csvurl'  : a.get('href'),
            'csvsize' : a.contents[2].strip()
            }
    else:
        return {}
def parseKMLInfoFromCol6(col):
    a = col.findAll('a')
    if len(a) > 0:
        a = a[0]
        return {
            'kmlurl'  : a.get('href'),
            'kmlsize' : a.contents[2].strip()
            }
    else:
        return {}
def parseShapeFromCol7(col):
    a = col.findAll('a')
    if len(a) > 0:
        a = a[0]
        return {
            'shapeurl'  : a.get('href'),
            'shapesize' : a.contents[2].strip()
            }
    else:
        return {}
def parseMapsFromCol8(col):
    a = col.findAll('a')
    if len(a) > 0:
        a = a[0]
        return {
            'mapsurl'  : a.get('href'),
            }
    else:
        return {}
def parseOtherFromCol9(col):
    a = col.findAll('a')
    if len(a) > 0:
        a = a[0]
        return {
            'otherurl'  : a.get('href'),
            }
    else:
        return {}
def parseCols(cols):
    '''Extract data and prep for insertion to db'''
    #import pdb
    #pdb.set_trace()
    data = {}
    data.update( parseLinkAndTextFromCol1(cols[0]))
    data.update( parseRatingFromCol2( cols[1]) )
    data.update( parseAgencyFromCol3( cols[2]) )
    data.update( parseXMLInfoFromCol4( cols[3]) )
    data.update( parseCSVInfoFromCol5( cols[4]) )
    data.update( parseKMLInfoFromCol6( cols[5]) )
    data.update( parseShapeFromCol7(  cols[6]) )
    data.update( parseMapsFromCol8(   cols[7]) )
    data.update( parseOtherFromCol9(  cols[8]) )
    return data

if __name__ == '__main__':
    fh =  open(FILENAME)
    strdata = "".join(fh.readlines())
    soup = BeautifulSoup(strdata)
    rows = soup.findAll("tr")

    parsed_data = []
    lineNo = 0 
    errno  = None
    for r in rows:
        try:
            cols = r.findAll("td")
            if len(cols) == 9:
                data = parseCols(cols)
                ins = dataset_table.insert().values(**data)
                conn = engine.connect()
                result = conn.execute(ins)
                parsed_data.append(data)
            else:
                print "********************************************************************************"
                print lineNo, " had nonstandard length"
                print "********************************************************************************"
        except Exception, e:
            errno = e
            print "********************************************************************************"
            print lineNo, " threw an exception"
            print "********************************************************************************"
        lineNo += 1
