from lxml import html, etree
import requests
import threading
import time
from igraph import Graph
import sqlite3
from sqlite3 import Error
import json
import re
import webcolors as webc
import logging
from subprocess import call


#logging.warning('Watch out!')  # will print a message to the console
#logging.info('I told you so')  # will not print anything


class mathPage:
    def __init__(self, mathID, verbose=False):
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
        # logging.info(f'Downloading info to mathID {self.id} from the online database.')
        print(f'Downloading info to mathID {self.id} from the online database.', end='\r')
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

    def kill_connection(self, conn, connection=None):
        if connection == None:
            conn.close()
        
        

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

    def insert_new_person(self, name, inst, year, diss, advisors, title, students={}, connection=None):
        cur,conn = self.get_cursor(connection)
        cur.execute("SELECT * FROM mathematicians ORDER BY id DESC LIMIT 1")
        res = cur.fetchall()
        lastID = res[0][0]
        if lastID > int(1e6):
            mathID = lastID+1
        else:
            mathID = int(1e6)
        if len(advisors) == 1:
            advisors.append(0)
        if len(advisors) == 2:
            advisors.append(0)            
        entry = (mathID, name, title, inst, year, diss, advisors[0], advisors[1], advisors[2], json.dumps(students))
        entry_update = (name, title, inst, year, diss, advisors[0], advisors[1], advisors[2], json.dumps(students), mathID)
        row = self.get_person(mathID, conn)
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
               """, (entry_update))
        conn.commit()
        if connection == None:
            conn.close()
        return(mathID)
            

        

            
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
        if connection == None:
            conn.close()
        return(students["MathID"])



    
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
            return((0,0,0,0,0,0,0,0,0,'{}'))
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

            

    def get_descendants(self, mathID, depth=0, connection=None):
        cur,conn = self.get_cursor(connection)
        i = 0
        desc = {mathID}
        desc_new = {mathID}
        if not self.exists(mathID):
            self.add_person(mathID)
        while True:
            desc_temp = set()
            if i > depth and not depth == 0:
                break
            for d in desc_new:
                p = self.get_person(d, connection=conn)                
                s = json.loads(p[9])
                if "MathID" in s.keys():
                    students = s["MathID"]
                    desc_temp = desc_temp | set(students)
            desc = desc | desc_new
            desc_new = desc_temp
            if len(desc_new) == 0:
                break
            i += 1
        if connection == None:
            conn.close()
        return(desc)
            
    def add_descendants(self, mathID, depth=0, connection=None):        
        cur,conn = self.get_cursor(connection)
        i = 0
        desc = {mathID}
        desc_new = {mathID}
        if not self.exists(mathID):
            self.add_person(mathID)
        while True:
            fetch = []
            desc_temp = set()
            if i >= depth and not depth == 0:
                break
            for d in desc_new:
                p = self.get_person(d, connection=conn)                
                s = json.loads(p[9])
                if "MathID" in s.keys():
                    students = s["MathID"]
                    desc_temp = desc_temp | set(students)
            desc = desc | desc_new
            desc_new = desc_temp
            if len(desc_new) == 0:
                break            
            for d in desc_new:
                if not self.exists(d):
                    fetch.append(d)
            self.populate_db(fetch, connection=connection)
            i += 1
        if connection == None:
            conn.close()
                

            
    def populate_db(self, limit, chunk = 10, connection=None):        
        cur,conn = self.get_cursor(connection)
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
        if connection == None:
            conn.close()


        

class mathGenealogy(Graph):

    def __init__(self, DB = "MathGen.db", name = "graph", vertices = None, directed=True):
        self.db = mathDB(DB)
        self.table_name = name
        self.db_file = DB
        self.init_db(name)
        self.roots = []
        super().__init__(directed=directed)
        self.vs["name"] = ""
        self.__rank = True
        self.__same_level = []
        self.__graphOptions = {}
        self.__fontsize = "12"
        self.__fontlarge = "14"
        self.__edgeOptions = {}
        self.__nodeOptions = {}
        self.__graphOptions['ratio'] = 'auto'
        self.__graphOptions['splines'] = 'spline'
        self.__graphOptions['fontname'] = 'helvetica'
        self.__graphOptions['bgcolor'] = 'white'
        self.__graphOptions['ranksep'] = "0.8"
        self.__graphOptions['newrank'] = "true"
        self.__graphOptions['mclimit'] = "100"
        self.__graphOptions['rankdir'] = 'TB'
        self.__graphOptions['minlen'] = "0.1"
        self.__graphOptions['nslimit'] = "10"
        self.__nodeOptions["shape"] = "box"
        self.__nodeOptions["rank"] = "same"
        self.__nodeOptions["style"] = "rounded, filled"
        self.__edgeOptions["penwidth"] = "2"
        self.__edgeOptions["arrowhead"] = "vee"

    def fixed_level(self, vIDs):
        if type(vIDs) == int:
            self.__same_level.append(vIDs)
        else:
            self.__same_level += vIDs


    def config_graph(self, ratio="auto", mclimit="100",
                     splines="spline", ranksep="1", fontname="helvetica",
                     bgcolor="#fffff0", rank=True):
        self.__graphOptions['ratio'] = ratio
        self.__graphOptions['splines'] = splines
        self.__graphOptions['fontname'] = fontname
        self.__graphOptions['bgcolor'] = bgcolor
        self.__graphOptions['ranksep'] = ranksep
        self.__graphOptions['mclimit'] = mclimit
        self.__rank = rank
       

    def config_nodes(self, style="rounded, filled", shape="box", rank="same"):
        self.__nodeOptions["shape"] = shape
        self.__nodeOptions["rank"] = rank
        self.__nodeOptions["style"] = style



    def config_edges(self, penwidth="2", arrowhead="vee"):
        self.__edgeOptions["penwidth"] = penwidth
        self.__edgeOptions["arrowhead"] = arrowhead


    

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

            

    # def create_table(self, conn, create_table_sql):
    #     """ create a table from the create_table_sql statement
    #     :param conn: Connection object
    #     :param create_table_sql: a CREATE TABLE statement
    #     :return:
    #     """
    #     try:
    #         c = conn.cursor()
    #         c.execute(create_table_sql)
    #     except Error as e:
    #         print(e)

    def vID_to_mathID(self, vID, connection=None):
        cur,conn = self.db.get_cursor(connection)
        if type(vID) == int:
            cur.execute(f"SELECT mathID FROM {self.table_name} WHERE vID = ?", (vID,))
            res = cur.fetchall()
            if len(res) > 0:
                return(res[0][0])
            else:
                return(0)
        elif type(vID) == list:
            res = []
            for i in vID:
                res.append(self.vID_to_mathID(i,conn))
            return(res)
        else:
            return(0)
                        


    def mathID_to_vID(self, mathID, connection=None):
        cur,conn = self.db.get_cursor(connection)
        cur.execute(f"SELECT vID FROM {self.table_name} WHERE mathID = ?", (mathID,))
        res = cur.fetchall()
        if len(res) > 0:
            return(res[0][0])
        else:
            return(0)        
        

    def get_person(self, vID, connection=None):
        cur,conn = self.db.get_cursor(connection)
        res = cur.execute(f"SELECT * FROM {self.table_name} WHERE vID = ?", (vID,))
        res = cur.fetchall()
        if connection == None:
            conn.close()
        if len(res) == 0:
            return((None,0,'{}'))
        else:
            return(res[0])

        
    def init_db(self, name, connection=None):
        cur,conn = self.db.get_cursor(connection)
        sql_create_graph_table = f"""CREATE TABLE IF NOT EXISTS {name} (
                                vID integer PRIMARY KEY,
                                mathID integer NOT NULL,
                                meta text
                              );"""
        if conn is not None:
            cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            conn.commit()
            self.db.create_table(conn, sql_create_graph_table)
            conn.close()
        else:
            print("Cannot create database connection")


    def insert_db_entry(self, mathID, vID, meta, connection = None):
        cur,conn = self.db.get_cursor(connection)
        row = self.get_person(vID, conn)
        meta_text = json.dumps(meta)
        if row[0] == None:
            cur.execute(f"INSERT INTO {self.table_name} VALUES (?,?,?)", (vID, mathID, meta_text))
        else:
            new_meta = {}
            old_meta = json.loads(self.get_person(vID, conn)[2])
            new_meta["roots"] = list(set(old_meta["roots"] + meta["roots"]))
            meta_text = json.dumps(new_meta)
            cur.execute(f"""
               UPDATE {self.table_name}
               SET mathID   = ?,
                   meta     = ?
               WHERE vID = ?
               """, (mathID, meta_text, vID))
        conn.commit()
        if connection == None:
            conn.close()

    def get_advisors(self, vID, connection=None):
        cur,conn = self.db.get_cursor(connection)
        query = f"""SELECT mathematicians.first_advisor,
                           mathematicians.second_advisor,
                           mathematicians.third_advisor
                    FROM mathematicians
                    JOIN {self.table_name}
                    ON mathematicians.id = {self.table_name}.mathID
                    WHERE vID = ?"""        
        cur.execute(query, (vID,))       
        res = cur.fetchall()
        if len(res) > 0:
            res = res[0]
        else:
            return(tuple())
        query = f"""SELECT vID FROM {self.table_name} WHERE mathID = ?"""
        adv = []
        for a in res:
            if a > 0:
                cur.execute(query, (a,))
                advID = cur.fetchall()[0][0]
                adv.append(advID)
        return(tuple(adv))
    

    def get_students(self, vID, connection=None):
        cur,conn = self.db.get_cursor(connection)
        query = f"""
        SELECT vID
        FROM {self.table_name}
        JOIN mathematicians
        ON mathematicians.id = graph.mathID
        WHERE first_advisor = (SELECT mathID FROM graph WHERE vID = ?)
        OR second_advisor = (SELECT mathID FROM graph WHERE vID = ?)
        OR third_advisor = (SELECT mathID FROM graph WHERE vID = ?)"""        
        cur.execute(query, (vID,vID,vID))
        res = cur.fetchall()
        stud = []
        for s in res:
            stud.append(s[0])
        return(stud)

    def add_new_link(self, vOUT, vIN):
        if self.get_eid(vOUT, vIN, error=False) < 0:
                    self.add_edge(vOUT, vIN)

        

    def add_links(self, vID):
        adv = self.get_advisors(vID)
        stud = self.get_students(vID)
        for a in adv:
            self.add_new_link(a, vID)
        for s in stud:
            self.add_new_link(vID,s)

    def add_all_links(self):
        for vID in range(len(self.vs)):
            self.add_links(vID)
        

    def add_person(self, mathID, root=None, line=None, force=False):
        person = self.db.get_person(mathID)
        if root != None and root not in self.roots:
            self.roots.append(root)
        if person[0] == 0:
            self.db.add_person(mathID)
            # p = mathPage(mathID)
            # p.get_info()
            # entry = p.get_entry()
            # if entry[1] == None:                
            #     raise ValueError("No such entry in the math genealogy project found.")
            #     return
            # else:    
            #     self.db.insert_person(p)
            person = self.db.get_person(mathID)
        if str(mathID) not in self.vs['name']:
            vID = self.add_vertex(str(mathID)).index
            self.add_links(vID)
            self._insert_person(person, vID, root, line)
        else:
            vID = self.vs.find(name=str(mathID)).index
            self.add_links(vID)
            if force:
                self._insert_person(person, vID, root, line)
            else:
                pass
#               self.vs[vID]["roots"].append(root)
        self.insert_db_entry(mathID, vID, {"roots": [root]})
            

    def add_ancestors(self, mathID, depth=0, root=None, line=None):
        self.db.add_ancestors(mathID, depth=depth)
        ancestors = self.db.get_ancestors(mathID, depth=depth)
        for a in ancestors:
            self.add_person(a, root=mathID)


    def add_descendants(self, mathID, depth=1, root=None, line=None):
        self.db.add_descendants(mathID, depth=depth)
        descendants = self.db.get_descendants(mathID, depth=depth)
        for d in descendants:
            self.add_person(d, root=mathID)


    def add_new_person(self, name, inst, year, diss, advisors, title, students={}, root=None):
        mathID = self.db.insert_new_person(name, inst, year, diss, advisors, title, students)
        self.add_person(mathID, root=root)
    

            
    def _insert_person(self, person, vID, root, line):
        students = json.loads(person[9])
        if len(students) > 0:
            students = students["MathID"]
            
        self.vs[vID]["Name"]           = person[1]
        self.vs[vID]["Title"]          = person[2]
        self.vs[vID]["Institution"]    = person[3]
        self.vs[vID]["Year"]           = str(person[4])
        self.vs[vID]["Dissertation"]   = person[5]
        self.vs[vID]["label"]          = self.__make_nice_label(vID)
        #self.vs[vID]["Students"]       = students
        #self.vs[vID]["Advisors"]       = person[6:8]


    def get_clusterID(self, vID, connection=None):
        out = 0
        p = self.get_person(vID, connection)
        roots = json.loads(p[2])["roots"]
        if roots == [None]:
            return(-1)
        for r in roots:
            out = out + 2**self.roots.index(r)
        return(out-1)
        
      
        
    def get_clusters(self, connection=None):
        alphabet = "ABCDEFGHIJKLMOPQRSTUVXYZ"
        cluster_dict = {}
        cluster = {}
        i = 0
        for r in self.roots:
            cluster_dict.update({alphabet[i]:[r]})
            i += 1
        for k,v in cluster_dict.items():
            cluster.update({k : [self.mathID_to_vID(v[0])]})
        for vID in range(len(self.vs)):
            p = self.get_person(vID, connection)
            roots = json.loads(p[2])["roots"]
            if roots == [None]:
                continue
            new = True
            for k,v in cluster_dict.items():
                if roots == v and vID not in cluster[k]:
                    cluster[k].append(vID)
                    new = False
                    break
            if new:
#                print(roots)
                key = []
                for r in roots:
#                    print(r)
                    for k,v in cluster_dict.items():
#                        print(k,v)
                        if len(k) == 1 and r in v:
                            key.append(k)
#                            print(key)
                key.sort()
                new_key = "".join(key)
#                print(new_key)
#                print("\n")
                cluster_dict.update({new_key: roots})
                cluster.update({new_key: [vID]})
        return(cluster_dict, cluster)

    def get_cluster_list(self):
        clusters = []
        for vID in range(len(self.vs)):
            c = self.get_clusterID(vID)
            self.vs[vID]["cluster"] = c
            if c not in clusters:
                clusters.append(c)
        return(clusters)

    def __cycle_color_list(self, colors, length):
        color_list = []
        col = []
        try:
            for c in colors:
                color_list.append(c)
        except TypeError:
            color_list.append(colors)
        for i in range(0, length):
            j = i % len(color_list)
#            print(j)
            col.append(color_list[j])
        return(col)


            

    def __get_color_CSS(self, color):
        if color[0] == "#" :
            col = color
        else:
            col = webc.name_to_hex(color, spec='css3')
        return(col)

    def color_graph_CSS(self, colors):
        rootCnt = len(self.roots)
        clusters = self.get_cluster_list()
        #print("Total number of clusters found: "+ str(len(clusters)))
        col = self.__cycle_color_list(colors, len(clusters))
        for vID in range(len(self.vs)):
            clusterID = self.get_clusterID(vID)
            v_col = self.__get_color_CSS(col[clusters.index(clusterID)])
            self.vs[vID]["color"] = v_col
            self.vs[vID]["fillcolor"] = v_col
            if self._get_lum(v_col) < 0.5:
                self.vs[vID]["fontcolor"] = "#ffffff"
            else:
                self.vs[vID]["fontcolor"] = "#000000"


    
            
        # if "name" in vertex.keys():
        #     self.add_vertex(vertex["name"])
        # else:
        #     print("Mandatory entry 'name' is missing")
        #     return()
        # for key in vertex:
        #     val = vertex[key]
        #     print(f"Dictionary contains {key} with value {val}")



    def __wrap_string(self, string, wrap):
        out='<i>'
        while (len(string)>wrap) :
            helpString=string[:(wrap+1)]
            i=helpString.rfind(' ')
            out = out + helpString[:i] + '</i><br/><i>'
            string=string[(i+1):]
        out = out + string + '</i>'
        return(out)


    def __wrap_institute(self, string):
        out=''
        while (string.find(' and ')>=0) :
            i=string.find('and')
            out = out + string[:(i-1)] + '<br/>and '
            string=string[(i+4):]
        out = out + string 
        return(out)


    # def __make_nice_label(self, vID):
    #     label = f"<<B><FONT point-size='18'>{self.vs[vID]['Name']}</FONT></B><BR/>"
    #     diss = self.vs[vID]["Dissertation"]
    #     if diss and not diss.isspace():
    #         line2 = self.__wrap_string(diss, 60) + "<BR/>"
    #         line2 = line2.replace("&", "&amp;")
    #         label = label + line2
    #     inst = self.vs[vID]["Institution"]
    #     if inst and not inst.isspace():
    #         inst = self.__wrap_institute(inst)
    #     else:
    #         inst = 'Unknown'
    #     year = self.vs[vID]["Year"]
    #     if not year or year.isspace():
    #         year = '?'        
    #     label= label + inst + ", <B>" + year + "</B>>"
    #     return(label)

    def __make_nice_label(self, vID):
        fn = "<font point-size='" + self.__fontsize + "'>"
        fl = "<font point-size='" + self.__fontlarge + "'>"
        nameLine = "<b><font point-size='"+ self.__fontlarge +"'>" + self.vs[vID]["Name"] + "</font></b>"
        instLine = ""
        dissLine = ""
        diss = self.vs[vID]["Dissertation"]
        inst = self.vs[vID]["Institution"]
        if inst and not inst.isspace():
            inst = self.__wrap_institute(inst)
            instLine = inst        
        year = self.vs[vID]["Year"]
        if year and not year.isspace():
            if instLine and not instLine.isspace():
                instLine = instLine + ", <b>" + year + "</b>"
            else:
                instLine = "<b>" + year + "</b>"
        if instLine or not instLine.isspace():
            instLine = "<br/>" + instLine
        if diss and not diss.isspace():
            diss = self.__wrap_string(diss, 60)
            dissLine = "<br/>" + diss.replace("&", "&amp;")
        if instLine or dissLine:
            label = "<" +fl + nameLine +"</font>" + fn + instLine + dissLine + "</font>" + ">"
        else:
            label = "<" +fl + nameLine +"</font>" + ">"
        return(label)
    

    def _hex2rgb(self, color):
        if color[0] == '#':
            r = int(color[1:3], 16)/255
            g = int(color[3:5], 16)/255
            b = int(color[5:7], 16)/255
            color = [r,g,b]
        return(color)

    def _rgb2hex(self, color):
        col = []
        colstr = []
        colout = '#'
        for i in range(0,3):
            col.append(hex(round(color[i]*255)))
            if len(col[i]) >3 :
                colout = colout + col[i][2:4]
#                colstr.append(col[i][2:4])
            else :
                colout = colout + '0' + col[i][2:3]
#                colstr.append('0' + col[i][2:3])        
        return(colout)

    def __blendColorsHex(self, col1, col2, t):
        col1 = self.__hex2rgb(col1)
        col2 = self.__hex2rgb(col2)
        col = []
        for i in range(0,3):
            col.append(math.sqrt((1-t) * pow(col1[i],2) + t * pow(col2[i], 2)))
        colstring = self.__rgb2hex(col)
        return(colstring)

    
    def __formatOptions(self, opts):
        out = "["
        optCnt = len(opts)
        i=0
        for opt in opts:
            out = out + opt + '="' + str(opts[opt]) + '"'
            if (optCnt-1) > i :
                out = out + ", "
            i += 1
        out = out + "]"
        return(out)
 

    
    def save(self, filename):
# Algorithm not optimal. Might redo labels although they already exist.
        self.add_all_links()
        for edge in self.es:
            if not "style" in edge.attribute_names() or not edge["style"]:
                edge["style"] = "solid"

#        self.__graphOptions["bgcolor"] = self.__getColCSS(self.__backColor)        
        for vID in range(len(self.vs)):            
#            if not self.vs[vxID].has_attr("label") or self.graph.vs[vxID]["label"].isspace():
            self.vs[vID]["label"] = self.__make_nice_label(vID)
#            if not self.graph.vs[vxID].has_attr("rank") or self.graph.vs[vxID]["rank"].isspace():
            if self.vs[vID]["Year"] and not self.vs[vID]["Year"].isspace():
                self.vs[vID]["rank"] = self.vs[vID]["Year"]
        self.write_dot(filename)
        with open(filename, 'r') as file :
            filedata = file.read()
        optionString = self.__formatOptions(self.__graphOptions)
        nodeOptionString = self.__formatOptions(self.__nodeOptions)
        edgeOptionString = self.__formatOptions(self.__edgeOptions)

        rankString = ""

        if self.__rank:
            rankString = '          {rank = "same";'
            for r in self.__same_level:
                rID = self.vs.find(name=str(r)).index
                rankString = rankString + str(rID) +';'
            rankString = rankString+'}'
        
        # Replace the target string
        break1 = filedata.find('\n') +1
        break2 = filedata[break1:].find('\n')
        filedata = filedata[:(break1+break2)] + " graph " + optionString  + "\n          node " + nodeOptionString + "\n          edge " + edgeOptionString + '\n' + rankString + filedata[(break1+break2):]
        filedata = filedata.replace('"<', '<')
        filedata = filedata.replace('>"', '>')
        # Write the file out again
        with open(filename, 'w') as file:
            file.write(filedata)

    def _get_lum(self, col):
        rgb = self._hex2rgb(col)
        lum = (0.2126*rgb[0] + 0.7152*rgb[1] + 0.0722*rgb[2])
        return(lum)


    def draw_graph(self, filename="graph.pdf", engine="pdf", bgcolor="white", clean=True):
        i = filename.rfind('.')
        if i > 0:
            fn = filename[:i]
        else:
            fn = filename
        if bgcolor != "white":
            self.__backColor = bgcolor
        if not filename:
            filename = "tmp_"+str(random.randint(1,9999999))
        self.save(fn+".dot")
        call(["dot", f"-T{engine}", f"{fn}.dot", "-o", f"{fn}.{engine}"])
        if clean:
            call(["rm", f"{fn}.dot"])
            
