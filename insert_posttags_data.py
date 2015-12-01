import MySQLdb
import sys

def splitter(st):
	curr_index = 0 
	l = []
	while(curr_index < len(st)):
		less_index = st.index('<',curr_index)
		great_index = st.index('>',curr_index)
		curr_index = great_index + 1
		temp  = st[less_index+1:great_index]
		l.append(temp)
	return l

try:
	db = MySQLdb.connect('<host-name>','<username>','<password>','<schema-name>')
	cursor = db.cursor()
  	cursor.execute("select Id, Tags from Posts")
  	# resultsult = db.use_result()
  	# res = result.fetch_row()[0]
  	res = cursor.fetchall()
  	#print type(res), res[0], len(res)
  	
  	final_result = []
  	temp_count = 0

  	for t in range(0,len(res)):
  		postid = res[t][0]
	  	tags = splitter(res[t][1])
	  	for tt in range(0,len(tags)):
	  		cursor_tag = db.cursor()
	  		query = "select Id, TagName from tags where TagName = '" + tags[tt] + "'"
	  		cursor_tag.execute(query)
	  		res_tag = cursor_tag.fetchone()
	  		final_result.append((postid,res_tag[0]))
			sql = """
					INSERT INTO PostTags(PostId,TagId)
		         	VALUES (%d,%d)
		         """ % (postid,res_tag[0])
			try:
			   cursor_insert = db.cursor()
			   cursor_insert.execute(sql)
			except:
			   db.rollback()
			   print "Rollback"
			   exit(0)

	print "Committed"
	db.commit()


	#print final_result

except MySQLdb.Error, e:
  # Print the error.
  print "ERROR %d: %s" % (e.args[0], e.args[1])
  sys.exit(1)
finally:
  # Close the connection when it is open.
  if db:
    db.close()