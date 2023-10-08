from lxml import html, etree
import requests
import threading
import time
from igraph import Graph
import sqlite3
from sqlite3 import Error
import json
import re
import logging
#logging.warning('Watch out!')  # will print a message to the console
#logging.info('I told you so')  # will not print anything


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
        self.students = "{}"

        
        
    def get_entry(self):
        return((self.id, self.name, self.title, self.inst, self.year, self.diss, self.advisorID[0], self.advisorID[1], self.advisorID[2], self.students))



    def get_info(self):
        logging.info(f'Downloading info to mathID {self.id} from the online database.')
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
            self.parse_page()        


            
    def checkPage(self):
        """
        Check if the page exists before parsing to avoid errors.
        """
        pass


    
    def parse_page(self):
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

        i = 1
        while True:
            try:
                descText = self.tree.xpath(f'//*[@id="paddingWrapper"]/p[{i}]/text()')
            except IndexError:
                break
            if ("According to our current" in descText[0]) or ("No students known" in descText[0]):
                break
            i += 1

        if ("No students known" in descText[0]):
            self.students = json.dumps({})
        else:            
            try:
                numStudents = int(''.join(filter(str.isdigit, descText[0])))
            except ValueError:
                numStudents = 0
            try:
                numDescendants = int(''.join(filter(str.isdigit, descText[1])))
            except ValueError:
                numDescendants = 0
            self.students = json.dumps({"MathID": studentsID, "Institute": studentsInst, "Year": studentsYear, "Students": numStudents, "Descendants": numDescendants})

        
        

class mathDB:
    
    def __init__(self, db_file):
        self.db_file = db_file
        self.init_db()


    def get_cursor(self, connection=None):        
        if connection == None:
            conn = self.create_connection()
        else:
            conn = connection
        cur = conn.cursor()
        return(cur, conn)
        
        

    def create_connection(self):
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

            

    def create_table(self, conn, create_table_sql):
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


        
    def init_db(self):
        conn = self.create_connection()
        sql_create_main_table = """CREATE TABLE IF NOT EXISTS mathematicians (
                                id integer PRIMARY KEY,
                                name text NOT NULL,
                                title text,
                                institute text,
                                year integer,
                                thesis text,
                                first_advisor integer,
                                second_advisor integer,
                                third_advisor integer,
                                students text
                              );"""
        if conn is not None:
            self.create_table(conn, sql_create_main_table)
            conn.close()
        else:
            print("Cannot create database connection")


    def add_person(self, mathID, connection=None):        
        if not self.exists(mathID):
            try:
                p = mathPage(mathID)
                p.get_info()
                self.insert_person(p, connection=connection)
            except ValueError:
                print(f"Error: No entry with mathID {mathID} exists.")
        

            
    def insert_person(self, mathPage, connection=None):
        entry = mathPage.get_entry()
        if entry[1] == None:
            raise ValueError("No such entry in the math genealogy project found.")
            return
        if connection == None:
            conn = self.create_connection()
        else:
            conn = connection
        cur = conn.cursor()
        row = self.get_person(mathPage.id, conn)
        if row[0] == 0:
            cur.execute("INSERT INTO mathematicians VALUES (?,?,?,?,?,?,?,?,?,?)", entry)
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
                   third_advisor  = ?,
                   students       = ?
               WHERE id = ?
               """, (mathPage.name, mathPage.title, mathPage.inst, mathPage.year, mathPage.diss, mathPage.advisorID[0], mathPage.advisorID[1],mathPage.advisorID[2], mathPage.students, mathPage.id))
        conn.commit()
        if connection == None:
            conn.close()
            

    def get_students(self, mathID, connection=None):
#        conn = sqlite3.connect(self.db_file)
        cur, conn = self.get_cursor(connection)
        students = []
        if not self.exists(mathID):
            try:
                self.add_person(mathID, connection=connection)
            except ValueError:
                print(f"Error: No entry with mathID {mathID} exists.")
                return(students)
        cur.execute("""
                       SELECT id
                       FROM mathematicians
                       WHERE first_advisor = ?
                       OR second_advisor = ?
                       OR third_advisor = ?
                   """, (mathID,mathID,mathID),)
        temp = cur.fetchall()
        for s in temp:
            students.append(s[0])
        if connection == None:
            conn.close()
        return(students)

    def get_students_entry(self, mathID, connection=None):
        cur, conn = self.get_cursor(connection)
        if not self.exists(mathID):
            try:
                self.add_person(mathID, connection)
            except ValueError:
                print(f"Error: No entry with mathID {mathID} exists.")
        p = self.get_person(mathID, connection=conn)
        students = json.loads(p[9])
        return(students["MathID"])
        if connection == None:
            conn.close()
        


    
    def find_missing(self, limit, connection=None):
        """
        Find missing entries in database. 
        """
#        conn = sqlite3.connect(self.db_file) 
        cur,conn = self.get_cursor(connection)
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
        if connection == None:
            conn.close()
        return(missing)


    
    def exists(self, mathID, connection=None):        
        cur,conn = self.get_cursor(connection)
        cur.execute("SELECT EXISTS(SELECT * FROM mathematicians WHERE id = ?)", (mathID,),)
        p = cur.fetchall()[0][0]        
        if connection == None:
            conn.close()
        if p == 1:
            return(True)
        else:
            return(False)


        
    def fetch_missing(self, limit):
        missing = self.find_missing(limit)
        if len(missing) > 0:
            self.populate_db(missing)


            
    def check_missing_data(self, id):
        cur,conn = self.get_cursor(connection)
        cur.execute("SELECT EXISTS(SELECT * FROM mathematicians WHERE id = ?)", (id,),)
        p = cur.fetchall()[0][0]
        if p == 0:
            if connection == None:
                conn.close()
            return((1,1,1,1,1,1,1,1))
        else:
            cur.execute("SELECT * FROM mathematicians WHERE id = ?", (id,),)
            data = cur.fetchall()[0]
            if connection == None:
                conn.close()
            return(data)
#            print(data)

               
        
    def get_person(self, id, connection=None):
        cur,conn = self.get_cursor(connection)
        res = cur.execute(f"SELECT * FROM mathematicians WHERE id = ?", (id,),)
        res = cur.fetchall()
        if connection == None:
            conn.close()
        if len(res) == 0:
            return((0,0,0,0,0,0,0,0,0,0))
        else:
            return(res[0])
            

    # def _makeLimitIterable(self, limit):
    #     if type(limit) == int:
    #         ret = range(1, limit+1)
    #     try:
    #         l = len(limit)
    #         ret = limit
    #     except TypeError as e:
    #         print("Wrong parameter. Limit must be an iterable object or an integer")
    #     return(ret)

    def get_ancestors(self, mathID, depth=0, connection=None):
        cur,conn = self.get_cursor(connection)
        i = 0
        anc = {mathID}
        anc_new = {mathID}
        if not self.exists(mathID):
            self.add_person(mathID)
        while True:
            anc_temp = set()
            if i > depth and not depth == 0:
                break
            for a in anc_new:
                p = self.get_person(a, connection=conn)
                for j in range(3):
                    if p[6+j] > 0:
                        anc_temp.add(p[6+j])
            anc = anc | anc_new
            anc_new = anc_temp
            if len(anc_new) == 0:
                break
            i += 1
        if connection == None:
            conn.close()
        return(anc)
    

    
    def add_ancestors(self, mathID, depth=0, connection=None):
        cur,conn = self.get_cursor(connection)
        i = 0
        anc = {mathID}
        anc_new = {mathID}
        if not self.exists(mathID):
            self.add_person(mathID)
        while True:
            anc_temp = set()            
            if i > depth and not depth == 0:
                break
            for a in anc_new:
                p = self.get_person(a, connection=connection)
                for j in range(3):
                    if p[6+j] > 0:
                        anc_temp.add(p[6+j])
#                        print(p[6+j])
            anc = anc | anc_new
            anc_new = anc_temp
            if len(anc_new) == 0:
                break
            for a in anc_new:
                if not self.exists(a):
                    self.add_person(a, connection=connection)
            i += 1
        if connection == None:
            conn.close()

    def add_decendants(self, mathID, depth=0, connection=None):
        pass
                

            
    def populate_db(self, limit, chunk = 10):
        conn = sqlite3.connect(self.db_file)
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
                thread = threading.Thread(target = persons[j].get_info)
                threads.append(thread)
                thread.start()
            for j, thread in enumerate(threads):
                thread.join()
            for j in range(chunk):
                print(f"Adding MathID {limit[i*chunk+j]} to database. Entry {i*chunk+j+1} of {l}.", end= '\r')
                self.insert_person(persons[j], conn)
        threads = list()
        persons = list()        
        for j in range(r):
            persons.append(mathPage(limit[n*chunk+j]))
        #    print(f"Creating MathID {limit[n*chunk+j]}")
            thread = threading.Thread(target = persons[j].get_info)
            threads.append(thread)
            thread.start()
        for j, thread in enumerate(threads):
            thread.join()
        for j in range(r):
            print(f"Adding MathID {limit[n*chunk+j]} to database. Entry {n*chunk+j+1} of {l}.", end= '\r')
            self.insert_person(persons[j], conn)
#            print(f"Downloading entry {i} of {limit}", end= '\r')
#            page = mathPage(i)
#            self.insert_person(page)
        conn.close()


        

class mathGenealogy(Graph):

    def __init__(self, DB="MathGen.db", vertices = None, directed=True):
        self.db = mathDB(DB)
        super().__init__(directed=directed)
        self.vs["name"] = ""

    def add_person(self, mathID, root=0, line=0, force=False):
        person = self.db.get_person(mathID)
        if person[0] == 0:
            p = mathPage(mathID)
            p.get_info()
            entry = p.get_entry()
            if entry[1] == None:                
                raise ValueError("No such entry in the math genealogy project found.")
                return
            else:    
                self.db.insert_person(p)
                person = self.db.get_person(mathID)
        if str(mathID) not in self.vs['name']:
            vID = self.add_vertex(str(mathID)).index
            self._insert_person(person, vID, root, line)

        elif force:
            vID = graph.vs.find(name=str(mathID)).index
            self._insert_person(person, vID, root, line)

            
    def _insert_person(self, person, vID, root, line):
        students = json.loads(person[9])
        self.vs[vID]["Name"]           = person[1]
        self.vs[vID]["Title"]          = person[2]
        self.vs[vID]["Year"]           = str(person[3])
        self.vs[vID]["Dissertation"]   = person[4]
        self.vs[vID]["Institution"]    = person[5]
        self.vs[vID]["Students"]       = students["MathID"]
        self.vs[vID]["Advisors"]       = person[6:8]
        self.vs[vID]["Line"] = line
        self.vs[vID]["roots"] = [root]
        
      
        
        


            
        # if "name" in vertex.keys():
        #     self.add_vertex(vertex["name"])
        # else:
        #     print("Mandatory entry 'name' is missing")
        #     return()
        # for key in vertex:
        #     val = vertex[key]
        #     print(f"Dictionary contains {key} with value {val}")



    def _wrap_string(self, string, wrap):
        out='<i>'
        while (len(string)>wrap) :
            helpString=string[:(wrap+1)]
            i=helpString.rfind(' ')
            out = out + helpString[:i] + '</i><br/><i>'
            string=string[(i+1):]
            out = out + string + '</i>'
        return(out)

    def _wrap_institute(self, string):
        out=''
        while (string.find(' and ')>=0) :
            i=string.find('and')
            out = out + string[:(i-1)] + '<br/>and '
            string=string[(i+4):]
        out = out + string 
        return(out)


    def _make_nice_label(self, vID):
        label = "<<b><font point-size='18'>" + self.vs[vID]["Name"] + "</font></b><br/>"
        diss = self.vs[vID]["Dissertation"]
        if diss and not diss.isspace():
            line2 = self._wrap_string(diss, 60) + "<br/>"
            line2 = line2.replace("&", "&amp;")
            label = label + line2
        inst = self.vs[vID]["Institution"]
        if inst and not inst.isspace():
            inst = self._wrap_institute(inst)
        else:
            inst = 'Unknown'
        year = self.vs[vID]["Year"]
        if not year or year.isspace():
            year = '?'        
        label= label + inst + ", <b>" + year + "</b>>"
        return(label)




total_entries = 297377 # number of records as of 2 October 2023

Greven_ID = 29360  # Andreas Greven
Aumann_ID= 36548  # Georg Aumann
Anita_ID = 92324 # Anita
Fourier_ID = 17981 # Fourier
Wolfgang_ID = 150286 # Wolfgang
Eichelsbacher_ID = 27275 # Peter
Anton_ID = 125956 # Anton
Pfaffelhuber_ID = 157881 # Peter Pfaffelhuber
Ruess_ID = 75966 # Ruess


if __name__ == "__main__":
    testDB = mathDB("test.db")
    test = mathPage(0)
