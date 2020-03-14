from flask import Flask
import os

UPLOAD_FOLDER = os.path.abspath(os.getcwd())+'/uploads'

spa_analyzer_flask = Flask(__name__)
spa_analyzer_flask.secret_key = "secret key"
spa_analyzer_flask.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
spa_analyzer_flask.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024


@spa_analyzer_flask.route('/')
def hello_world():
    return 'SPA Analyzer Flask'


if __name__ == '__main__':
    spa_analyzer_flask.run()
