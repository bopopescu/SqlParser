from blue import create_app,create_MySQL_connection
from flask import Flask, render_template, url_for, redirect, jsonify

app = create_app()

mysql = create_MySQL_connection(app)

if __name__ == '__main__':
    app.run(debug=True)