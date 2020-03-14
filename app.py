from flask import Flask
import os

UPLOAD_FOLDER = os.path.abspath(os.getcwd())+'/uploads'

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024


@app.route('/')
def index_page():
    return 'SPA Analyzer Flask'


if __name__ == '__main__':
    app.run()
