import sys, os, json
lib_path = os.path.abspath(os.path.join('../libs/raco', '../libs/myria-python'))
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
SNAPSHOT_LIST=['000045' '000054' '000072']
USER_NAME="jortiz"
PROGRAM_NAME="romulustest"

#parameters
DM_SOL_UNIT = 7.374469e13
NON_GRP_PARTICLES = '-1'

#queries
#implied: grp, time, mass * dm_sol_unit, totalParticles
EXTRA_ATTRIBUTES = []
#END CONFIGURE

connection = MyriaConnection(hostname = "rest.myria.cs.washington.edu", port=1776, ssl=True)

#Write the union
for i in SNAPSHOT_LIST:
	#dbscan query

#global myrial query
