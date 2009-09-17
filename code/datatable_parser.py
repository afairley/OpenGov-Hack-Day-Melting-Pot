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
#        [ ] fetch and parse the other pages?
#        [ ] use urlib and beautiful soup to make this a proper scraper rather than wget and vim :D

FILENAME =  ("/home/vegas/Documents/Projects" + #replace this absolute path 
             "/hackday/opengov/"              + #replace this absolute path
             "data/datatable.html")             #relative path for datafile

from BeautifulSoup import BeautifulSoup

def parseLinkAndTextFromCol1(col):
    col = col.contents[0]
    link       = col.findAll('a')[0]
    relUrl     = link.get('href')
    relUrlName = link.firstText()
    
    text = col.findAll('span')[0]
    text = text.contents[0]
    return {
             'relUrl'     : relUrl,
             'relUrlName' : relUrlName,
             'text'       : text
           }

def parseCols(cols):
    '''Extract data and prep for insertion to db'''
    #import pdb
    #pdb.set_trace()
    return parseLinkAndTextFromCol1(cols[0])



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
            struct = parseCols(cols)
            parsed_data.append(struct)
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
