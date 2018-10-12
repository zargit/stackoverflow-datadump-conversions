import sqlite3
import bs4
import time
import datetime

"""
The following schema is based on the specification mentioned here https://ia800107.us.archive.org/27/items/stackexchange/readme.txt

It has the following tables - 

* users - (https://archive.org/download/stackexchange/stackoverflow.com-Users.7z)
* posts - (https://archive.org/download/stackexchange/stackoverflow.com-Posts.7z)
* posthistory - (https://archive.org/download/stackexchange/stackoverflow.com-PostHistory.7z)
* postlinks - (https://archive.org/download/stackexchange/stackoverflow.com-PostLinks.7z)
* tags - (https://archive.org/download/stackexchange/stackoverflow.com-Tags.7z)
* badges - (https://archive.org/download/stackexchange/stackoverflow.com-Badges.7z)
* comments - (https://archive.org/download/stackexchange/stackoverflow.com-Comments.7z)
* votes - (https://archive.org/download/stackexchange/stackoverflow.com-Votes.7z)

"""


database_file = "stack.db" # This file will be created in the same directory as this script if it doesn't exist.

create_badges = "CREATE TABLE IF NOT EXISTS badges (\
	id integer PRIMARY KEY,\
	userid integer,\
	name text,\
	date integer,\
	class integer,\
	tagbased text\
);"

create_comments = "CREATE TABLE IF NOT EXISTS comments (\
	id integer PRIMARY KEY,\
	userid integer,\
	postid integer,\
	text text,\
	score integer,\
	creationdate integer\
);"

create_posthistory = "CREATE TABLE IF NOT EXISTS posthistory (\
	id integer PRIMARY KEY,\
	userid integer,\
	postid integer,\
	posthistorytypeid integer,\
	comment text,\
	userdisplayname text,\
	text text,\
	revisionguid text,\
	creationdate integer,\
	closereasonid integer\
);"

create_postlinks = "CREATE TABLE IF NOT EXISTS postlinks (\
	id integer PRIMARY KEY,\
	postid integer,\
	relatedpostid integer,\
	linktypeid integer,\
	creationdate integer\
);"

create_posts = "CREATE TABLE IF NOT EXISTS posts (\
	id integer PRIMARY KEY,\
	posttypeid integer,\
	parentid integer,\
	tags text,\
	score integer,\
	commentcount integer,\
	title text,\
	owneruserid integer,\
	ownerdisplayname text,\
	favoritecount integer,\
	viewcount integer,\
	answercount integer,\
	acceptedanswerid integer,\
	body text,\
	communityowneddate integer,\
	lasteditordisplayname text,\
	lasteditoruserid integer,\
	creationdate integer,\
	lastactivitydate integer,\
	lasteditdate integer,\
	closeddate integer\
);"

create_tags = "CREATE TABLE IF NOT EXISTS tags (\
	id integer PRIMARY KEY,\
	tagname text,\
	count integer,\
	wikipostid integer,\
	excerptpostid integer\
);"

create_users = "CREATE TABLE IF NOT EXISTS users (\
	id integer PRIMARY KEY,\
	displayname text,\
	accountid integer,\
	aboutme text,\
	age integer,\
	emailhash text,\
	websiteurl text,\
	profileimageurl text,\
	reputation integer,\
	upvotes integer,\
	downvotes integer,\
	views integer,\
	creationdate integer,\
	lastaccessdate integer,\
	location text\
);"

create_votes = "CREATE TABLE IF NOT EXISTS votes (\
        id integer PRIMARY KEY,\
	postid integer,\
        votetypeid integer,\
        creationdate integer,\
        userid integer,\
        bountyamount integer\
);"

"""
The extracted xml files must be in the same directory as this script for the converion to work properly.
Also the xml filenames should conform to the names mentioned below in the `data_files` dict.

"""

create_tables = [create_users, create_tags, create_posts, create_posthistory, create_postlinks, create_comments, create_badges, create_votes]
data_files = { 'badges':"Badges.xml", 'comments':"Comments.xml", 'posts':"Posts.xml", 'posthistory':"PostHistory.xml", 'postlinks':"PostLinks.xml", 'tags':"Tags.xml", 'users':"Users.xml", 'votes':"Votes.xml"}
schema = { 'badges':create_badges, 'comments':create_comments, 'posts':create_posts, 'posthistory':create_posthistory, 'postlinks':create_postlinks, 'tags':create_tags, 'users':create_users, 'votes':create_votes}

"""
Columns with the following names (as in the `dates` list) will be converted to unix timestamp during conversion..
"""
dates = ['date', 'creationdate', 'communityowneddate', 'lastactivitydate', 'lastaccessdate', 'lasteditdate', 'closeddate']

def get_timestamp(str):
	return time.mktime(datetime.datetime.strptime(str, "%Y-%m-%dT%H:%M:%S.%f").timetuple())	

conn = sqlite3.connect(database_file)

for table in schema:

	c = conn.cursor()
	c.execute(schema[table])

	print("Executed:", schema[table])
	
	with open(data_files[table]) as fd:
		count = 0
		for line in fd.readlines():
			if len(line.strip()) > 0:
				row = bs4.BeautifulSoup(line, 'html.parser')
				row = row.row
				
				sql = "INSERT INTO "+table+" ( "
				cols = []
				vals = []
				for col in row.attrs:
					cols.append(col)
					if col in dates:
						vals.append(int(get_timestamp(row[col])))
					elif col in ['class']:
						## This condition is due to bs4 parsing class attribute as an array
						vals.append(row[col][0])
					elif row[col].isdigit():
						vals.append(int(row[col]))
					else:
						vals.append(row[col])

				sql += ','.join(cols) + " ) VALUES ( "+','.join('?'*len(vals))+" )"
				c.execute(sql, vals)
				count += 1
				if( count % 1000 == 0):
					print(table+":", str(count)+" rows inserted ... ")
		print("Total", count, "rows inserted")		

conn.commit()
conn.close()
