import json
import sqlite3      

#Do NOT put functions/statement outside functions

def start():
    
    #import JSON into DB
    file = 'airbnb.json';
    with open(file, 'r',encoding="utf8") as myfile:
        data=myfile.read()

    # parse file
    listing = json.loads(data)

    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()

    #Creates table 'reviewer' and inserts values
    c.execute("DROP TABLE IF EXISTS reviewer")
    c.execute('''CREATE TABLE reviewer
        (rid INTEGER PRIMARY KEY, rname text)''')
    rid_inserted = []
    for i in listing:
        review = i["reviews"]
            
        for r in review:
            rid = r["reviewer_id"]
            rname = r["reviewer_name"]
            
            #c.execute("SELECT rid FROM reviewer WHERE rid = (?)", (rid, ))
            #rid_found = c.fetchall()
            #if not rid_found:
            
            if rid not in rid_inserted:
                rid_inserted += [rid]
                c.execute("INSERT INTO reviewer (rid, rname) VALUES (?,?)",(rid, rname))
    conn.commit()

    #Creates table 'accommodation' and inserts values
    c.execute("DROP TABLE IF EXISTS accommodation")
    c.execute('''CREATE TABLE accommodation
        (id INTEGER PRIMARY KEY, name text, summary text, url text, review_score_value INTEGER)''')

    for i in listing:
        id = i["_id"]
        
        name = i["name"]
        summary= i["summary"]
        url = i["listing_url"]
        if i["review_scores"]:
            r_s_v = i["review_scores"]["review_scores_value"]
            c.execute("INSERT INTO accommodation (id, name, summary, url, review_score_value) VALUES (?,?,?,?,?)",(id,name,summary,url,r_s_v))
        else:
            c.execute("INSERT INTO accommodation (id, name, summary, url) VALUES (?,?,?,?)",(id,name,summary,url))
    conn.commit()

    #Creates table 'host' and inserts values
    c.execute("DROP TABLE IF EXISTS host")
    c.execute('''CREATE TABLE host
        (host_id INTEGER PRIMARY KEY, host_url text, host_name text, host_about text, host_location text)''')

    
    host_inserted = []
    for i in listing:
        host_id = i["host"]["host_id"]
        host_url = i["host"]["host_url"]
        host_name = i["host"]["host_name"]
        host_about = i["host"]["host_about"]
        host_location = i["host"]["host_location"]

        if host_id not in host_inserted:
            host_inserted += [host_id]
            c.execute("INSERT INTO host (host_id, host_url, host_name, host_about, host_location) VALUES (?,?,?,?,?)",(host_id, host_url, host_name, host_about, host_location))
    conn.commit()

    #Creates table 'host_accommodation' and inserts values
    c.execute("DROP TABLE IF EXISTS host_accommodation")
    c.execute('''CREATE TABLE host_accommodation
        (host_id INTEGER, accommodation_id INTEGER, PRIMARY KEY (host_id, accommodation_id),
         CONSTRAINT fk_column
             FOREIGN KEY (host_id) REFERENCES host (host_id),
         CONSTRAINT fk_column
             FOREIGN KEY (accommodation_id) REFERENCES accommodation (accommodation_id))''')

    for i in listing:
        host_id = i["host"]["host_id"]
        accommodation_id = i["_id"]
        c.execute("INSERT INTO host_accommodation (host_id, accommodation_id) VALUES (?,?)", (host_id, accommodation_id))
    conn.commit()


    #Creates table 'amenities' and inserts values
    c.execute("DROP TABLE IF EXISTS amenities")
    c.execute('''CREATE TABLE amenities
        (accommodation_id INTEGER, type text, PRIMARY KEY (accommodation_id, type),
         CONSTRAINT fk_column
             FOREIGN KEY (accommodation_id) REFERENCES accommodation (accommodation_id))''')

    
    for i in listing:
        type_exists = []
        accommodation_id = i["_id"]
        amenities = i["amenities"]
        
        for type in amenities:
            if type not in type_exists:
                type_exists += [type]
                c.execute("INSERT INTO amenities (accommodation_id, type) VALUES (?,?)",(accommodation_id, type))
    conn.commit()

    #Creates table 'review' and inserts values
    c.execute("DROP TABLE IF EXISTS review")
    c.execute('''CREATE TABLE review
        (id INTEGER PRIMARY KEY autoincrement, rid INTEGER, comment text, datetime text, accommodation_id INTEGER,
         CONSTRAINT fk_column
             FOREIGN KEY (accommodation_id) REFERENCES accommodation (accommodation_id),
         CONSTRAINT fk_column
             FOREIGN KEY (rid) REFERENCES reviewer (rid))''')
    
    for i in listing:
        accommodation_id = i["_id"]
        reviews = i["reviews"]
        for r in reviews:
            rid = r["reviewer_id"]
            comment = r["comments"]
            datetime = r["date"]["$date"]
            c.execute("INSERT INTO review (rid, comment, datetime, accommodation_id) VALUES (?, ?,?,?)",(rid, comment, datetime, accommodation_id))
    conn.commit()
    
    conn.close()


if __name__ == '__main__':
    start()

