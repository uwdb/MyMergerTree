import sys, os, json
import time
lib_path = os.path.abspath(os.path.join('../../libs/raco', '../../libs/myria-python'))
sys.path.append(lib_path)
from raco.catalog import FromFileCatalog
import raco.myrial.parser as parser
import raco.myrial.interpreter as interpreter
import raco.algebra as alg
from raco.expression.expression import UnnamedAttributeRef
from myria import MyriaConnection
from myria import MyriaSchema
from myria import MyriaRelation

#CONFIGURE: information about the datasets in Myria
SNAPSHOT_LIST=['000045','000054','000072']
USER_NAME="jortiz"
PROGRAM_NAME="romulustest"
#END CONFIGURE

connection = MyriaConnection(hostname = "rest.myria.cs.washington.edu", port=1776, ssl=True)

#first get schemas, create the catalog
f = open('schema.py', 'w')
f.write("{" + '\n');
for i in SNAPSHOT_LIST:
	current_relation = USER_NAME + ":" + PROGRAM_NAME + ":" + "cosmo" + i;
	current_schema = (MyriaRelation(relation=current_relation, connection=connection).schema.to_dict())

	columnNames = [x.encode('utf-8') for x in current_schema['columnNames']]
	columnTypes = [x.encode('utf-8') for x in current_schema['columnTypes']]

	columns = zip(columnNames, columnTypes)
	f.write("'" + current_relation + "' : " +  str(columns) + ',\n');
f.write("}" + '\n');
f.close()

catalog = FromFileCatalog.load_from_file("schema.py")
_parser = parser.Parser()

#second, create the query
for i in SNAPSHOT_LIST:
	current_relation = USER_NAME + ":" + PROGRAM_NAME + ":" + "cosmo" + i;
	current_query = "T1 = scan("+ current_relation + "); store(T1," + current_relation + ");"

	statement_list = _parser.parse(current_query);
	processor = interpreter.StatementProcessor(catalog, True)
	processor.evaluate(statement_list)

	#modify and get the plans
	p = processor.get_logical_plan()
	tail = p.args[0].input
	p.args[0].input = alg.Shuffle(tail, [UnnamedAttributeRef(0)])
	p = processor.get_physical_plan()
	finalplan = processor.get_json()
	
	#submit the query
	print "/***** HASHING SNAPSHOT " + current_relation  + "*****/";
	query_status= connection.submit_query(finalplan)
	query_id = query_status['queryId']
	status = (connection.get_query_status(query_id))['status']

	#keep checking, sleep a little
	while status!='SUCCESS':
		status = (connection.get_query_status(query_id))['status']
		time.sleep(5);
	if status=='ERROR':
		break;

	if status=='SUCCESS':
		print 'QUERY SUCCESS'
	else:
		print 'QUERY ERROR'
