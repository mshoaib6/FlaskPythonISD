from flask import Flask, request,jsonify
import json
import sqlite3
import datetime

app = Flask(__name__)
app.config['DEBUG'] = True

#Do NOT put functions/statement outside functions

# Show your student ID
@app.route('/mystudentID/', methods=['GET'])
def my_student_id():    
    response={"studentID": "18079999D"}
    return jsonify(response), 200, {'Content-Type': 'application/json'}

@app.route('/airbnb/reviews/', methods=['GET'])
def reviews():
    if 'start' in request.args.keys() and 'end' in request.args.keys():
        start = request.args.get('start')
        end = request.args.get('end')
        
        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()
        c.execute("SELECT rid, comment, datetime, accommodation_id FROM review ORDER BY datetime DESC, rid ASC")
        rows = c.fetchall()
        count = 0
        listOfDict = []
        start = datetime.date(int(start[:4]), int(start[5:7]), int(start[8:]))
        end = datetime.date(int(end[:4]), int(end[5:7]), int(end[8:]))

        for i in rows:
            thisDate = i[2]
            thisDate = datetime.date(int(thisDate[:4]), int(thisDate[5:7]), int(thisDate[8:10]))
            if (thisDate >= start) and (thisDate <= end):
                count = count + 1
                this_rid = i[0] 
                c.execute("SELECT rname FROM reviewer WHERE rid = (?)", (this_rid, ))
                this_rname = c.fetchall()
                thisDict = {'Accommodation ID' :i[3],
                            'Comment': i[1],
                            'DateTime' : i[2],
                            'Reviewer ID' : i[0],
                            'Reviewer Name' : this_rname[0][0]}
                dictCopy = thisDict.copy()
                listOfDict.append(dictCopy)

        response = {'Count' : count,
                    'Reviews' : listOfDict}
        conn.close()
        return jsonify(response), 200, {'Content-Type': 'application/json'}

    elif 'start' not in request.args.keys() and 'end' not in request.args.keys():
        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()
        c.execute("SELECT rid, comment, datetime, accommodation_id FROM review ORDER BY datetime DESC, rid ASC")
        rows = c.fetchall()
        count = 0
        listOfDict = []
        for i in rows:
            count = count + 1
            this_rid = i[0] 
            c.execute("SELECT rname FROM reviewer WHERE rid = (?)", (this_rid, ))
            this_rname = c.fetchall()
            thisDict = {'Accommodation ID' : i[3],
                        'Comment': i[1],
                        'DateTime' : i[2],
                        'Reviewer ID' : i[0],
                        'Reviewer Name' : this_rname[0][0]}
            dictCopy = thisDict.copy()
            listOfDict.append(dictCopy)
        response = {'Count' : count,
                    'Reviews' : listOfDict}
        conn.close()
        return jsonify(response), 200, {'Content-Type': 'application/json'}
    else:
        response = {'Reasons' : [{'Message' : "Wrong query parameters"}]}
        return jsonify(response), 404, {'Content-Type': 'application/json'}

@app.route('/airbnb/reviewers/', methods=['GET'])
def reviewers():

    if 'sort_by_review_count' in request.args.keys():
        sort_by_review_count = request.args.get('sort_by_review_count')

        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()
        c.execute("SELECT rid, rname FROM reviewer ORDER BY rid ASC")
        rows = c.fetchall()

        count = 0
        listOfDict = []

        c.execute("DROP TABLE IF EXISTS reviewer_detail")
        c.execute('''CREATE TABLE reviewer_detail
                (rid INTEGER PRIMARY KEY, rname text, num_of_reviews INTEGER)''')

        for i in rows:
            
            c.execute("SELECT * FROM review WHERE rid = (?)", (i[0],))
            noOfReviews = c.fetchall()
            noOfReviews = len(noOfReviews)

            c.execute("INSERT INTO reviewer_detail (rid, rname, num_of_reviews) VALUES (?,?,?)",(int(i[0]), i[1], noOfReviews))

        conn.commit()

        if sort_by_review_count == "ascending":
            c.execute("SELECT rid, rname, num_of_reviews FROM reviewer_detail ORDER BY num_of_reviews ASC, rid ASC")
        elif sort_by_review_count == "descending":
            c.execute("SELECT rid, rname, num_of_reviews FROM reviewer_detail ORDER BY num_of_reviews DESC, rid ASC")
        else:
            c.execute("DROP TABLE IF EXISTS reviewer_detail")
            conn.commit()
            response = {'Reasons' : [{'Message' : "Wrong query parameters"}]}
            conn.close()
            return jsonify(response), 404, {'Content-Type': 'application/json'}

        
        newRows = c.fetchall()

        for i in newRows:
            count = count + 1
            thisDict = {'Review Count' : i[2],
                        'Reviewer ID': i[0],
                        'Reviewer Name': i[1]}
            dictCopy = thisDict.copy()
            listOfDict.append(dictCopy)

        c.execute("DROP TABLE IF EXISTS reviewer_detail")
            
        response = {'Count' : count,
                    'Reviewers' : listOfDict}
        
        conn.commit()
        conn.close()
        return jsonify(response), 200, {'Content-Type': 'application/json'}
        

    else:
        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()
        c.execute("SELECT rid, rname FROM reviewer ORDER BY rid ASC")
        rows = c.fetchall()

        count = 0
        listOfDict = []
        
        for i in rows:
            count = count + 1
            c.execute("SELECT * FROM review WHERE rid = (?)", (i[0],))
            noOfReviews = c.fetchall()
            noOfReviews = len(noOfReviews)
            thisDict = {'Review Count' : noOfReviews,
                        'Reviewer ID': i[0],
                        'Reviewer Name': i[1]}
            dictCopy = thisDict.copy()
            listOfDict.append(dictCopy)
        response = {'Count' : count,
                    'Reviewers' : listOfDict}
        conn.close()
        return jsonify(response), 200, {'Content-Type': 'application/json'}

@app.route('/airbnb/reviewers/<reviewerID>', methods=['GET'])
def get_reviewer_detail(reviewerID):
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    reviewerID = int(reviewerID)
    listOfDict = []
    c.execute("SELECT * FROM review WHERE rid = (?) ORDER BY datetime DESC", (reviewerID,))
    rows = c.fetchall()
    if len(rows) > 0:   
        c.execute("SELECT rname FROM reviewer WHERE rid = (?)", (reviewerID,))
        rname = c.fetchall()
        for i in rows:
            thisDict = {'Accommodation ID' : i[4],
                        'Comment': i[2],
                        'DateTime': i[3]}
            dictCopy = thisDict.copy()
            listOfDict.append(dictCopy)
        response = {'Reviewer ID' : reviewerID,
                    'Reviewer Name' : rname[0][0],
                    'Reviews' : listOfDict}
        return jsonify(response), 200, {'Content-Type': 'application/json'}
    else:
        response = {'Reasons' : [{'Message' : "Reviewer not found"}]}
        return jsonify(response), 404, {'Content-Type': 'application/json'}

    conn.commit()
    conn.close()


@app.route('/airbnb/hosts/', methods=['GET'])
def hosts():
    if 'sort_by_accommodation_count' in request.args.keys():
        sort_by_accommodation_count = request.args.get('sort_by_accommodation_count')
        
        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()

        count = 0
        listOfDict = []

        c.execute("SELECT * FROM host ORDER BY host_id ASC")
        rows = c.fetchall()

        c.execute("DROP TABLE IF EXISTS host_detail")
        c.execute('''CREATE TABLE host_detail
                    (host_id INTEGER PRIMARY KEY, host_url text, host_name text,
                    host_about text, host_location text, num_of_accommodations INTEGER)''')


        for i in rows:

            c.execute("SELECT * FROM host_accommodation WHERE host_id = (?)", (i[0],))
            noOfAccomodation = c.fetchall()
            noOfAccomodation = len(noOfAccomodation)

            c.execute("INSERT INTO host_detail (host_id, host_url, host_name, host_about, host_location, num_of_accommodations) VALUES (?,?,?,?,?,?)",(i[0],i[1],i[2],i[3],i[4],noOfAccomodation))

        conn.commit()

        if sort_by_accommodation_count == "ascending":
            c.execute("SELECT host_id, host_url, host_name, host_about, host_location, num_of_accommodations FROM host_detail ORDER BY num_of_accommodations ASC, host_id ASC")
        elif sort_by_accommodation_count == "descending":
            c.execute("SELECT host_id, host_url, host_name, host_about, host_location, num_of_accommodations FROM host_detail ORDER BY num_of_accommodations DESC, host_id ASC")
        else:
            c.execute("DROP TABLE IF EXISTS host_detail")
            conn.commit()
            response = {'Reasons' : [{'Message' : "Wrong query parameters"}]}
            conn.close()
            return jsonify(response), 404, {'Content-Type': 'application/json'}

        newRows = c.fetchall()
        for i in newRows:
            count = count + 1
            thisDict = {'Accommodation Count' : i[5],
                        'Host About': i[3],
                        'Host ID' : i[0],
                        'Host Location': i[4],
                        'Host Name': i[2],
                        'Host URL': i[1]
                        }
            dictCopy = thisDict.copy()
            listOfDict.append(dictCopy)

        c.execute("DROP TABLE IF EXISTS host_detail")
            
        response = {'Count' : count,
                    'Hosts' : listOfDict}

        conn.commit()
        conn.close()

        return jsonify(response), 200, {'Content-Type': 'application/json'}
    else:
        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()

        count = 0
        listOfDict = []

        c.execute("SELECT * FROM host ORDER BY host_id ASC")
        rows = c.fetchall()


        for i in rows:
            count = count + 1
            c.execute("SELECT * FROM host_accommodation WHERE host_id = (?)", (i[0],))
            noOfAccomodation = c.fetchall()
            noOfAccomodation = len(noOfAccomodation)
            thisDict = {'Accommodation Count' : noOfAccomodation,
                        'Host About': i[3],
                        'Host ID' : i[0],
                        'Host Location': i[4],
                        'Host Name': i[2],
                        'Host URL': i[1]
                        }
            dictCopy = thisDict.copy()
            listOfDict.append(dictCopy)

            
        response = {'Count' : count,
                    'Hosts' : listOfDict}
        return jsonify(response), 200, {'Content-Type': 'application/json'}
        conn.commit()
        conn.close()

@app.route('/airbnb/hosts/<hostID>', methods=['GET'])
def get_host_detail(hostID):
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    hostID = int(hostID)

    c.execute("SELECT accommodation_id FROM host_accommodation WHERE host_id = (?) ORDER BY accommodation_id ASC", (hostID,))
    accommodationID = c.fetchall()
    count = 0

    accomoDict = []
    if len(accommodationID) > 0:   

        for i in accommodationID:
            c.execute("SELECT name FROM accommodation WHERE id = (?)", (i[0],))
            names = c.fetchall()
            for j in names:
                count = count + 1
                thisDict = {'Accommodation ID':i[0],
                            'Accommodation Name': j[0]}
                dictCopy = thisDict.copy()
                accomoDict.append(dictCopy)
        
        c.execute("SELECT * FROM host WHERE host_id = (?)", (hostID,))
        hostDetails = c.fetchall()
        
        response = {'Accommodation' : accomoDict,
                    'Accommodation Count' : count,
                    'Host About' : hostDetails[0][3],
                    'Host ID' : hostDetails[0][0],
                    'Host Location' : hostDetails[0][4],
                    'Host Name' : hostDetails[0][2],
                    'Host URL' : hostDetails[0][1]}
        return jsonify(response), 200, {'Content-Type': 'application/json'}
    else:
        response = {'Reasons' : [{'Message' : "Host not found"}]}
        return jsonify(response), 404, {'Content-Type': 'application/json'}

@app.route('/airbnb/accommodations/', methods=['GET'])
def accommodations():
    if 'min_review_score_value' in request.args.keys() and 'amenities' in request.args.keys():
        min_review_score_value = int(request.args.get('min_review_score_value'))
        amenities = request.args.get('amenities')
        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()

        c.execute("SELECT * FROM accommodation ORDER BY id ASC")
        accomodations = c.fetchall()
        count = 0
        responseList = []

        for i in accomodations:
            amenitiesList = []
            accomoID = i[0]
            if (i[4]):
                if (i[4] >= min_review_score_value):
                    
                    c.execute("SELECT type FROM amenities WHERE accommodation_id = (?)", (accomoID,))
                    amenitiess = c.fetchall()
                    
                    if (amenitiess):
                        for j in amenitiess:
                            amenitiesList.append(j[0])
                        
                        if amenities in amenitiesList:
                            
                            count = count + 1
                            accomoDict = {'Name' :  i[1],
                                          'Summary' :  i[2],
                                          'URL' : i[3]}
                            
                            


                            c.execute("SELECT host_id FROM host_accommodation WHERE accommodation_id = (?)", (accomoID,))
                            hostID = c.fetchall()
                            hostID = hostID[0][0]
                            c.execute("SELECT * FROM host WHERE host_id = (?)", (hostID,))
                            hostDetails = c.fetchall()
                            
                            
                            hostDetailDict = {'About' : hostDetails[0][3],
                                              'ID' : hostID,
                                              'Location' : hostDetails[0][4],
                                              'Name' : hostDetails[0][2]}
                            c.execute("SELECT * FROM review WHERE accommodation_id = (?)", (accomoID,))
                            reviewCount = c.fetchall()
                            reviewCount = len(reviewCount)

                            responseDict = {'Accommodation' : accomoDict,
                                        'Amenities' : amenitiesList,
                                        'Host' : hostDetailDict,
                                        'ID' : accomoID,
                                        'Review Count' : reviewCount,
                                        'Review Score Value' : i[4]}
                            dictCopy = responseDict.copy()
                            responseList.append(dictCopy)

        response = {'Accommodations' : responseList,
                    'Count' : count}

        return jsonify(response), 200, {'Content-Type': 'application/json'}
    if 'min_review_score_value' in request.args.keys() and 'amenities' not in request.args.keys():
        min_review_score_value = int(request.args.get('min_review_score_value'))
        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()

        c.execute("SELECT * FROM accommodation ORDER BY id ASC")
        accomodations = c.fetchall()
        count = 0
        responseList = []

        for i in accomodations:
            amenitiesList = []
            accomoID = i[0]
            if (i[4]):
                if (i[4] >= min_review_score_value):
                    
                    c.execute("SELECT type FROM amenities WHERE accommodation_id = (?)", (accomoID,))
                    amenitiess = c.fetchall()
                    
                    if (amenitiess):
                        for j in amenitiess:
                            amenitiesList.append(j[0])
                        
                        
                            
                    count = count + 1
                    accomoDict = {'Name' :  i[1],
                                'Summary' :  i[2],
                                'URL' : i[3]}
                            
                            


                    c.execute("SELECT host_id FROM host_accommodation WHERE accommodation_id = (?)", (accomoID,))
                    hostID = c.fetchall()
                    hostID = hostID[0][0]
                    c.execute("SELECT * FROM host WHERE host_id = (?)", (hostID,))
                    hostDetails = c.fetchall()
                            
                            
                    hostDetailDict = {'About' : hostDetails[0][3],
                                        'ID' : hostID,
                                        'Location' : hostDetails[0][4],
                                        'Name' : hostDetails[0][2]}
                    c.execute("SELECT * FROM review WHERE accommodation_id = (?)", (accomoID,))
                    reviewCount = c.fetchall()
                    reviewCount = len(reviewCount)

                    responseDict = {'Accommodation' : accomoDict,
                                'Amenities' : amenitiesList,
                                'Host' : hostDetailDict,
                                'ID' : accomoID,
                                'Review Count' : reviewCount,
                                'Review Score Value' : i[4]}
                    dictCopy = responseDict.copy()
                    responseList.append(dictCopy)

        response = {'Accommodations' : responseList,
                    'Count' : count}

        return jsonify(response), 200, {'Content-Type': 'application/json'}
    if 'min_review_score_value' not in request.args.keys() and 'amenities' in request.args.keys():

        amenities = request.args.get('amenities')
        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()

        c.execute("SELECT * FROM accommodation ORDER BY id ASC")
        accomodations = c.fetchall()
        count = 0
        responseList = []

        for i in accomodations:
            amenitiesList = []
            accomoID = i[0]

                    
            c.execute("SELECT type FROM amenities WHERE accommodation_id = (?)", (accomoID,))
            amenitiess = c.fetchall()
                    
            if (amenitiess):
                for j in amenitiess:
                    amenitiesList.append(j[0])
                        
                if amenities in amenitiesList:
                            
                    count = count + 1
                    accomoDict = {'Name' :  i[1],
                                    'Summary' :  i[2],
                                    'URL' : i[3]}
                            
                            


                    c.execute("SELECT host_id FROM host_accommodation WHERE accommodation_id = (?)", (accomoID,))
                    hostID = c.fetchall()
                    hostID = hostID[0][0]
                    c.execute("SELECT * FROM host WHERE host_id = (?)", (hostID,))
                    hostDetails = c.fetchall()
                            
                            
                    hostDetailDict = {'About' : hostDetails[0][3],
                                        'ID' : hostID,
                                        'Location' : hostDetails[0][4],
                                        'Name' : hostDetails[0][2]}
                    c.execute("SELECT * FROM review WHERE accommodation_id = (?)", (accomoID,))
                    reviewCount = c.fetchall()
                    reviewCount = len(reviewCount)

                    responseDict = {'Accommodation' : accomoDict,
                                'Amenities' : amenitiesList,
                                'Host' : hostDetailDict,
                                'ID' : accomoID,
                                'Review Count' : reviewCount,
                                'Review Score Value' : i[4]}
                    dictCopy = responseDict.copy()
                    responseList.append(dictCopy)

        response = {'Accommodations' : responseList,
                    'Count' : count}

        return jsonify(response), 200, {'Content-Type': 'application/json'}
    else:
        conn = sqlite3.connect("airbnb.db")
        c = conn.cursor()

        c.execute("SELECT * FROM accommodation ORDER BY id ASC")
        accomodations = c.fetchall()
        count = 0
        responseList = []

        for i in accomodations:
            count = count + 1
            accomoDict = {'Name' :  i[1],
                          'Summary' :  i[2],
                          'URL' : i[3]}
            
            accomoID = i[0]

            c.execute("SELECT type FROM amenities WHERE accommodation_id = (?)", (accomoID,))
            amenities = c.fetchall()
            amenitiesList = []
            if (amenities):
                for j in amenities:
                    amenitiesList.append(j[0])

            c.execute("SELECT host_id FROM host_accommodation WHERE accommodation_id = (?)", (accomoID,))
            hostID = c.fetchall()
            hostID = hostID[0][0]
            c.execute("SELECT * FROM host WHERE host_id = (?)", (hostID,))
            hostDetails = c.fetchall()
            
            
            hostDetailDict = {'About' : hostDetails[0][3],
                              'ID' : hostID,
                              'Location' : hostDetails[0][4],
                              'Name' : hostDetails[0][2]}
            c.execute("SELECT * FROM review WHERE accommodation_id = (?)", (accomoID,))
            reviewCount = c.fetchall()
            reviewCount = len(reviewCount)

            responseDict = {'Accommodation' : accomoDict,
                        'Amenities' : amenitiesList,
                        'Host' : hostDetailDict,
                        'ID' : accomoID,
                        'Review Count' : reviewCount,
                        'Review Score Value' : i[4]}
            dictCopy = responseDict.copy()
            responseList.append(dictCopy)

        response = {'Accommodations' : responseList,
                    'Count' : count}
        return jsonify(response), 200, {'Content-Type': 'application/json'}
        conn.commit()
        conn.close()

@app.route('/airbnb/accommodations/<accommodationID>', methods=['GET'])
def get_accommodation_detail(accommodationID):
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    amenitiesList = []

    c.execute("SELECT * FROM accommodation WHERE id = (?)", (accommodationID,))
    detail = c.fetchall()
    if len(detail) > 0:
        c.execute("SELECT type FROM amenities WHERE accommodation_id = (?)", (accommodationID,))
        amenities = c.fetchall()
        amenitiesList = []
        if (amenities):
            for j in amenities:
                amenitiesList.append(j[0])
        amenitiesList.sort()
        c.execute("SELECT * FROM review WHERE accommodation_id = (?) ORDER BY datetime DESC", (accommodationID,))
        reviews = c.fetchall()
        reviewsList = []
        for i in reviews:
            c.execute("SELECT rname FROM reviewer WHERE rid = (?)", (i[1],))
            rname = c.fetchall()
            rname = rname[0][0]
            thisReview = {'Comment' : i[2],
                          'DateTime' : i[3],
                          'Reviewer ID' : i[1],
                          'Reviewer Name' : rname}
            reviewsList.append(thisReview)
        response = {'Accommodation ID' : accommodationID,
                    'Accommodation Name' : detail[0][1],
                    'Amenities' : amenitiesList,
                    'Review Score Value' : detail[0][4],
                    'Reviews' : reviewsList,
                    'Summary' : detail[0][2],
                    'URL' : detail[0][3]}
        return jsonify(response), 200, {'Content-Type': 'application/json'}
        conn.commit()
        conn.close()
    else:
        response = {'Reasons' : [{'Message' : "Accommodation not found"}]}
        return jsonify(response), 404, {'Content-Type': 'application/json'}
    

if __name__ == '__main__':
   app.run()

