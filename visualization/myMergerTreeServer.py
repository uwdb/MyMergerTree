import requests
import json
from flask import request
from flask import Flask
app = Flask(__name__, static_url_path='')


@app.route('/')
def root():
    return app.send_static_file('index.html')


@app.route('/login', methods=['POST', 'GET'])
def login():
    error = None
    if request.method == 'POST':
        if valid_login(request.form['username'],
                       request.form['password']):
            return log_the_user_in(request.form['username'])
        else:
            error = 'Invalid username/password'

if __name__ == '__main__':
    # turn off if production
    app.debug = True
    app.run()