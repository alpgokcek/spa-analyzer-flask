from flask import Flask, request, redirect, jsonify
import os
from werkzeug.utils import secure_filename
from helpers import analyze_spa, delete_excel, gat_analyzer
import threading
from multiprocessing.pool import ThreadPool
import concurrent.futures
from ThreadWithReturn import ThreadWithReturnValue
import requests
from dotenv import load_dotenv
from os import environ

load_dotenv()

UPLOAD_FOLDER = os.path.abspath(os.getcwd()) + '/uploads'

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
latest_file_path = ''


@app.route('/')
def index_page():
    return 'SPA Analyzer Flask'


ALLOWED_EXTENSIONS = {'xls', 'xlsx', 'xlsm'}

@app.after_request
def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    return response

def allowed_file(filename):
    if '.' in filename:
        file_ext = str(filename.rsplit('.', 1)[1].lower())
        for extension in ALLOWED_EXTENSIONS:
            if file_ext.__contains__(extension):
                return True
    return False


def get_file_name(filename):
    if '.' in filename:
        splitted_file_name = filename.rsplit('.', 1)
        return [str(splitted_file_name[0].lower()), str('.' + splitted_file_name[1].lower())]


def check_auth():
    r = requests.get(environ.get('LARAVEL_URL') + '/api/check-auth',
                     headers={'Authorization': str(request.headers.get('Authorization'))})
    # file_path = ''
    if r.status_code != 204:
        resp = jsonify({'message': 'Auth failed.'})
        resp.status_code = r.status_code
        return resp
    return None

@app.route('/file-upload', methods=['POST'])
def upload_file():
    auth_status = check_auth()
    if auth_status is not None: return auth_status
    r = requests.get(environ.get('LARAVEL_URL')+'/api/check-auth', headers={'Authorization': str(request.headers.get('Authorization'))})
    # file_path = ''
    if r.status_code != 204:
        resp = jsonify({'message': 'Auth failed.'})
        resp.status_code = r.status_code
        return resp
    if request.json:
        section, department, year_and_term, type, code, name, credit = request.json.section, request.json.department, request.json.year_and_term, request.type, request.code, request.name, request.credit
    elif request.form:
        section, department, year_and_term, type, code, name, credit = request.form['section'], request.form['department'], request.form['year_and_term'], \
                                                              request.form['type'], request.form['code'], request.form[
                                                                  'name'], request.form['credit']
    else:
        resp = jsonify({'message': 'No department or year_and_term provided with request body'})
        resp.status_code = 400
        return resp

    if 'file' not in request.files or request.files['file'].filename == '':
        resp = jsonify({'message': 'No file provided with the request'})
        resp.status_code = 400
        return resp
    file = request.files['file']
    if file and allowed_file(file.filename):
        filename = get_file_name(secure_filename(file.filename))
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], str(hash(filename[0])) + filename[1])
        file.save(file_path)
        resp = jsonify({'message': 'File successfully uploaded'})
        resp.status_code = 201

        try:
            if type == 'spa':
                thread1 = ThreadWithReturnValue(target=analyze_spa,
                                                args=(file_path, section, department, code, year_and_term, name, credit))
                thread1.start()
                return_val = thread1.join()

            else:
                thread1 = ThreadWithReturnValue(target=gat_analyzer,
                                                args=(file_path, section, department, code, year_and_term, name, credit))
                thread1.start()
                return_val = thread1.join()

            if return_val is not True:
                resp = jsonify({'error': 'An error occurred. New changes won\'t be applied', 'message': return_val})
                resp.status_code = 500
                delete_excel(department, code, year_and_term, name, credit)
            os.remove(file_path)
        except Exception as e:
            resp = jsonify({'error': 'An error occurred.', 'message': e})
            resp.status_code = 500
            return resp
        return resp
    else:
        resp = jsonify({'message': 'Allowed file types are pdf, xls, xlsx, xlsm'})
        resp.status_code = 400
        return resp


@app.route('/file-remove', methods=['DELETE'])
def remove_file():
    auth_status = check_auth()
    if auth_status is not None: return auth_status
    try:
        if request.json:
            section, department, code, year_and_term, name, credit = request.json.section, request.json.department, request.json.code, request.json.year_and_term, request.json.name, request.json.credit
        else:
            section, department, code, year_and_term, name, credit = request.form['section'], request.form['department'], request.form['code'], \
                                                            request.form['year_and_term'], request.form['name'], \
                                                            request.form['credit']
        delete_excel(section, department, code, year_and_term, name, credit)
        resp = jsonify({})
        resp.status_code = 205
    except Exception as e:
        resp = jsonify({'error': 'An error occurred.', 'message': e})
        resp.status_code = 500
    return resp


if __name__ == "__main__":
    app.run()
