from flask import Flask
from flask_mysqldb import MySQL
import yaml
from flask_cors import CORS

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(__name__)

    db = yaml.load(open('db.yaml'))
    app.config['MYSQL_HOST'] = db['mysql_host']
    app.config['MYSQL_USER'] = db['mysql_user']
    app.config['MYSQL_PASSWORD'] = db['mysql_password']
    app.config['MYSQL_DB'] = db['mysql_db']

    #mysql = MySQL(app)
    CORS(app)

    from blue.api.routes import modapi
    # from blue.site.routes import mod

    # app.register_blueprint(site.routes.mod)
    app.register_blueprint(modapi, url_prefix='/api')

    from blue.api import routes
    app.register_blueprint(routes)

    return app

def create_MySQL_connection(app):
    return MySQL(app)