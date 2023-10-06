from lxml import html, etree
import requests
import threading
import time
import igraph as ig
import sqlite3
from sqlite3 import Error


class mathPage:
    def __init__(self, mathID):
        """
        Create an empty object only containing the ID
        """
        self.id = mathID
        self.mirror = "http://mathgenealogy.org/"
        self.url = self.mirror+"id.php?id="+str(mathID)
        self.name = None
        self.title = None
        self.inst = None
        self.year = None
        self.diss = None
        self.advisorID = [None, None, None]

    def getEntry(self):
        return((self.id, self.name, self.title, self.inst, self.year, self.diss, self.advisorID[0], self.advisorID[1], self.advisorID[2]))

    def getInfo(self):
        temp = requests.get(self.url)
        temp.encoding = "utf-8"
        self.page = temp.text
        if "Please back up and try again" in temp.text:
            self.name = "Error"
            self.title = None
            self.inst = None
            self.diss = None
            self.advisorID = [None, None, None]
        else: 
            self.tree = html.fromstring(self.page)
            self.parsePage()        
        
    def checkPage(self):
        """
        Check if the page exists before parsing to avoid errors.
        """
        pass

    def parsePage(self):
        name1 = self.tree.xpath('//*[@id="paddingWrapper"]/h2/text()')
        self.name = str(name1[0]).replace("  "," ").strip()

        title1 = self.tree.xpath('//*[@id="paddingWrapper"]/div[2]/span/text()[1]')
        title2 = str(title1[0])
        if title2 and not title2.isspace():
            self.title = title2.strip()
        else:
            self.title = " "
            
        inst1 = self.tree.xpath('//*[@id="paddingWrapper"]/div[2]/span/span/text()')
        if len(inst1)>0:
            inst2 = str(inst1[0])
        else:
            inst2 = " "
        if inst2 and not inst2.isspace():
            self.inst = inst2.strip()
        else:
            self.inst = " "

        year1 = self.tree.xpath('//*[@id="paddingWrapper"]/div[2]/span/text()[2]')
        year2 = str(year1[0])
        if year2 and not year2.isspace():
            self.year = year2.strip()
        else:
            self.year = " "

        diss1 = self.tree.xpath('//*[@id="thesisTitle"]/text()')
        diss2 = str(diss1[0])
        if diss2 and not diss2.isspace():
            self.diss = diss2.strip()
        else:
            self.diss = " "

        ### May be problematic but unused at the moment
        advisor1 = self.tree.xpath('//*[@id="paddingWrapper"]/p[2]/a/text()')
        advisor = list()
        for s in advisor1:            
            advisor.append(str(s).replace("  "," "))
        ### Problematic part    ----------------
        advisor1 = self.tree.xpath('//*[@id="paddingWrapper"]/p[3]/a/text()')
        for s in advisor1:
            if "\n" not in s:
        #        print(s)  ### Debugging
                advisor.append(str(s).replace("  "," "))
        #----------------------------------------
           
        
        self.advisor = advisor

        

        advisorID1 = self.tree.xpath('//*[@id="paddingWrapper"]/p[2]/a/@href')
        advisorID = list()
        for s in advisorID1:
            advisorID.append(int(str(s)[10:]))
        advisorID1 = self.tree.xpath('//*[@id="paddingWrapper"]/p[3]/a/@href')
        for s in advisorID1:            
            if "Chrono" not in s:
                advisorID.append(int(str(s)[10:]))


        if len(advisorID) == 0:
            advisorID.append(0)
        if len(advisorID) == 1:
            advisorID.append(0)
        if len(advisorID) == 2:
            advisorID.append(0)
                
        self.advisorID = advisorID

        studentsID1 = self.tree.xpath('//*[@id="paddingWrapper"]/table/tr/td[1]/a/@href')
        studentsID = list()
        for s in studentsID1:
            studentsID.append(int(str(s)[10:]))
        self.studentsID = studentsID

        studentsInst1 = self.tree.xpath('//*[@id="paddingWrapper"]/table/tr/td[2]/text()')
        studentsInst = list()
        for s in studentsInst1:
            studentsInst.append(str(s))
        self.studentsInst = studentsInst

        studentsYear1 = self.tree.xpath('//*[@id="paddingWrapper"]/table/tr/td[3]/text()')
        studentsYear = list()
        for s in studentsYear1:
            studentsYear.append(s)
        self.studentsYear = studentsYear

class mathDB:
    def __init__(self, db_file):
        self.db_file = db_file
        self.initDB()

    def createConnection(self):
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            #print(sqlite3.version)
        except Error as e:
            print(e)
        finally:
            if conn:
                return(conn)

    def createTable(self, conn, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)


        
    def initDB(self):
        conn = self.createConnection()
        sql_create_main_table = """CREATE TABLE IF NOT EXISTS mathematicians (
                                id integer PRIMARY KEY,
                                name text NOT NULL,
                                title text,
                                institute text,
                                year integer,
                                thesis text,
                                first_advisor integer,
                                second_advisor integer,
                                third_advisor integer
                              );"""
        if conn is not None:
            self.createTable(conn, sql_create_main_table)
            conn.close()
        else:
            print("Cannot create database connection")

    def insertPerson(self, mathPage):
        conn = self.createConnection()
        cur = conn.cursor()
        row = self.getPerson(mathPage.id)
        if len(row) == 0:
            cur.execute("INSERT INTO mathematicians VALUES (?,?,?,?,?,?,?,?,?)", mathPage.getEntry())
        else:
            cur.execute("""
               UPDATE mathematicians
               SET name           = ?,
                   title          = ?,
                   institute      = ?,
                   year           = ?,
                   thesis         = ?,
                   first_advisor  = ?,
                   second_advisor = ?,
                   third_advisor  = ?                
               WHERE id = ?
               """, (mathPage.name, mathPage.title, mathPage.inst, mathPage.year, mathPage.diss, mathPage.advisorID[0], mathPage.advisorID[1],mathPage.advisorID[2], mathPage.id))
        conn.commit()
        conn.close()

    def findMissing(self, limit):
        """
        Find missing entries in database. 
        """
        conn = sqlite3.connect(self.db_file)
        cur = conn.cursor()
        missing = []
        if type(limit) == int:
            limit = range(1, limit+1)
        try:
            l = len(limit)
        except TypeError as e:
            print("Wrong parameter. Limit must be an iterable object or an integer")

        for i in limit:
            if type(i) == int:
                cur.execute("SELECT EXISTS(SELECT * FROM mathematicians WHERE id = ?)", (i,))
                p = cur.fetchall()[0][0]
                #print(p)
            else:
                print("Error: wrong data Type of limit parameter")
                return(missing)
            if p == 0:
                missing.append(i)
        conn.close()
        return(missing)

    def exists(self, id):
        conn = sqlite3.connect(self.db_file)
        cur = conn.cursor()
        cur.execute("SELECT EXISTS(SELECT * FROM mathematicians WHERE id = ?)", (id,))
        p = cur.fetchall()[0][0]        
        conn.close()
        if p == 1:
            return(True)
        else:
            return(False)

    def fetchMissing(self, limit):
        missing = self.findMissing(limit)
        if len(missing) > 0:
            self.populateDB(missing)

    def checkMissingData(self, id):
        conn = sqlite3.connect(self.db_file)
        cur = conn.cursor()
        cur.execute("SELECT EXISTS(SELECT * FROM mathematicians WHERE id = ?)", (id,))
        p = cur.fetchall()[0][0]
        if p == 0:
            conn.close()
            return((1,1,1,1,1,1,1,1))
        else:
            cur.execute("SELECT * FROM mathematicians WHERE id = ?", (id,))
            data = cur.fetchall()[0]
            conn.close()
            return(data)
#            print(data)

        
    
    
        
    def getPerson(self, id):
        conn = self.createConnection()
        cur = conn.cursor()
        res = cur.execute(f"SELECT * FROM mathematicians WHERE id = {id}")
        res = cur.fetchall()
        conn.close()
        return res

    # def _makeLimitIterable(self, limit):
    #     if type(limit) == int:
    #         ret = range(1, limit+1)
    #     try:
    #         l = len(limit)
    #         ret = limit
    #     except TypeError as e:
    #         print("Wrong parameter. Limit must be an iterable object or an integer")
    #     return(ret)

            
    def populateDB(self, limit, chunk = 10):
        if type(limit) == int:
            limit = range(1, limit+1)
        try:
            l = len(limit)
        except TypeError as e:
            print("Wrong parameter. Limit must be an iterable object or an integer")
        n = l // chunk
        r = l % chunk
        for i in range(n):
            threads = list()
            persons = list()
            for j in range(chunk):
                persons.append(mathPage(limit[i*chunk+j]))
            #    print(f"Creating MathID {limit[i*chunk+j]}")
                thread = threading.Thread(target = persons[j].getInfo)
                threads.append(thread)
                thread.start()

            for j, thread in enumerate(threads):
                thread.join()

            for j in range(chunk):
                print(f"Adding MathID {limit[i*chunk+j]} to database. Entry {i*chunk+j+1} of {l}.", end= '\r')
                self.insertPerson(persons[j])

        threads = list()
        persons = list()        
        for j in range(r):
            persons.append(mathPage(limit[n*chunk+j]))
        #    print(f"Creating MathID {limit[n*chunk+j]}")
            thread = threading.Thread(target = persons[j].getInfo)
            threads.append(thread)
            thread.start()

        for j, thread in enumerate(threads):
            thread.join()

        for j in range(r):
            print(f"Adding MathID {limit[n*chunk+j]} to database. Entry {n*chunk+j+1} of {l}.", end= '\r')
            self.insertPerson(persons[j])
#            print(f"Downloading entry {i} of {limit}", end= '\r')
#            page = mathPage(i)
#            self.insertPerson(page)
    

class mathGenealogy:
    pass

total_entries = 297377 # number of records as of 2 October 2023


if __name__ == "__main__":
    testDB = mathDB("test.db")
    test = mathPage(0)
