from app import app
from app import myMergerTreeQueries as mmt
from flask import render_template, url_for, redirect, request, session, g, flash, jsonify, send_from_directory
import requests
import json
import os

# log = logging.getLogger('werkzeug')
# log.setLevel(logging.DEBUG)

@app.route('/')
@app.route('/index')
def root():
    return render_template('index.html')

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static', 'images'),
                               'favicon.ico')

@app.route('/verifytables', methods=['GET'])
def verify_table_input():
    print "IN VERIFY USER"
    if mmt.verify_table(table=request.args.get('nodestable')) and mmt.verify_table(table=request.args.get('edgestable')):
        print "LEAVING VERIFY USER TRUE"
        return jsonify(verified='TRUE')
    else:
        print "LEAVING VERIFY USER FALSE"
        return jsonify(verified='FALSE')

@app.route('/myriaquery', methods=['GET'])
def myria_query():
    qtype = request.args.get('querytype')
    print "QUERY TYPE", qtype
    if qtype == 'init_timesteps':
        timesteps = mmt.get_unique_times(table=request.args.get('nodesTable'))
        if timesteps is None:
            'RETURNING TIMESTEP FALSE'
            return jsonify(query_status='ERROR')
        else:
            print 'RETURNING TIMESTEP TRUE'
            # print timesteps
            return jsonify(query_status='SUCCESS', timesteps=timesteps)
    elif qtype == 'get_nowgroups_by_mass':
        nowGroups = mmt.get_nowgroups_by_mass(user=request.args.get('user'), table=request.args.get('nodesTable'), timeStepAttr=request.args.get('timeStepAttr'), nowGroupAttr=request.args.get('nowGroupAttr'), massAttr=request.args.get('massAttr'), minMass=request.args.get('minMass'), maxMass=request.args.get('maxMass'))
        # print nowGroups
        if nowGroups is None:
            'RETURNING NOWGROUPS FALSE'
            return jsonify(query_status='ERROR')
        else:
            print 'RETURNING NOWGROUPS TRUE'
            return jsonify(query_status='SUCCESS', nowGroups=nowGroups)
    elif qtype == 'get_mergertree':
        print "GETTING MERGER TREE " + request.args.get('group')
        [nodes, edges] = mmt.get_mergertree(user=request.args.get('user'), nodesTable=request.args.get('nodesTable'), edgesTable=request.args.get('edgesTable'), nowGroupAttr=request.args.get('nowGroupAttr'), group=request.args.get('group'))
        if nodes is None or edges is None:
            print 'RETURNING MERGERTREE FALSE'
            return jsonify(query_status='ERROR')
        else:
            print 'RETURNING MERGERTREE TRUE'
            return jsonify(query_status='SUCCESS', nodes=nodes, edges=edges)
