from flask import Flask, request, redirect, jsonify
import os
import urllib.request
from werkzeug.utils import secure_filename
from helpers import analyze_spa
import celery

worker = celery.Celery('spa-analyzer-flask')
app.conf.update(BROKER_URL=os.environ['REDIS_URL'],
                CELERY_RESULT_BACKEND=os.environ['REDIS_URL'])

UPLOAD_FOLDER = os.path.abspath(os.getcwd())+'/uploads'
print(UPLOAD_FOLDER)

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024


@app.route('/')
def index_page():
    return 'SPA Analyzer Flask'


ALLOWED_EXTENSIONS = set(['pdf', 'xls', 'xlsx', 'xlsm'])


def allowed_file(filename):
    if '.' in filename:
        file_ext = str(filename.rsplit('.', 1)[1].lower())
        for extension in ALLOWED_EXTENSIONS:
            if file_ext.__contains__(extension):
                return True
    return False

@worker.task
def deneme(file_path):
    return analyze_spa(file_path)

@app.route('/file-upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        resp = jsonify({'message': 'No file part in the request'})
        resp.status_code = 400
        return resp
    file = request.files['file']
    if file.filename == '':
        resp = jsonify({'message': 'No file selected for uploading'})
        resp.status_code = 400
        return resp
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        resp = jsonify({'message': 'File successfully uploaded'})
        analyze_spa(file_path)
        resp.status_code = 201
        return resp
    else:
        resp = jsonify({'message': 'Allowed file types are pdf, xls, xlsx, xlsm'})
        resp.status_code = 400
        return resp


if __name__ == "__main__":
    app.run()
