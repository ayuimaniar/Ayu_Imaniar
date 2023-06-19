from flask import Flask, jsonify, request, g
import pandas as pd
from flasgger import Swagger, swag_from, LazyJSONEncoder
from modules.cleaner_func import clean_text, cleansing_files
from modules.db import *
from modules.config import Config
from time import perf_counter

def initialize_database():
    db_connection = create_connection()
    insert_dictionary_to_db(db_connection)
    db_connection.close()

def get_db():
    if 'db_connection' not in g:
        g.db_connection = create_connection()
    return g.db_connection

def create_app():
    app = Flask(__name__) 
    app.json_encoder = LazyJSONEncoder
    app.config.from_object(Config)
    Swagger(app, template=app.config['SWAGGER_TEMPLATE'], config=app.config['SWAGGER_CONFIG'])
    return app

app = create_app()

@app.teardown_appcontext
def close_db(exception=None):
    db_connection = g.pop('db_connection', None)

    if db_connection is not None:
        db_connection.close()

@app.route('/', methods=['GET'])
@swag_from('docs_yml/home.yml', methods=['GET'])
def home():
    return jsonify({
        "version": "1.0.0",
        "message": "Welcome to Flask API",
        "author": "Ayu Imaniar"
    })

@app.route('/cleaned_result', methods=['GET'])
@swag_from('docs_yml/cleaned_result.yml', methods=['GET'])
def show_cleansing_result_api():
    db_connection = get_db()
    cleansing_result = show_cleansing_result(db_connection)
    return jsonify(cleansing_result)

@app.route('/text_cleansing_form', methods=['POST'])
@swag_from('docs_yml/text_cleansing_form.yml', methods=['POST'])
def cleansing_form():
    raw_text = request.form["raw_text"]
    start = perf_counter()
    cleaned_text = clean_text(raw_text)
    end = perf_counter()
    time_elapsed = end - start
    result = {"raw_text": raw_text, "clean_text": cleaned_text, "processing_time": time_elapsed}
    db_connection = get_db()
    insert_result_to_db(db_connection, raw_text, cleaned_text)
    return jsonify(result)

@app.route('/file_cleansing_upload', methods=['POST'])
@swag_from('docs_yml/file_cleansing_upload.yml', methods=['POST'])
def cleansing_upload():
    upload_file = request.files['upload_file']
    df_upload = pd.read_csv(upload_file, encoding='latin-1')
    df_cleansing = cleansing_files(upload_file)
    db_connection = get_db()
    insert_upload_result_to_db(db_connection, df_cleansing)
    result = df_cleansing.to_dict(orient='records')
    return jsonify(result)

if __name__ == '__main__':
    initialize_database()
    app.run(
        debug=app.config['DEBUG'],
        use_debugger=app.config['USE_DEBUGGER'],
        use_reloader=app.config['USE_RELOADER'],
        port=app.config["PORT"]
    )
