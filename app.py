from blue import create_app
from flask import Flask, render_template, url_for, redirect, jsonify

app = create_app()


if __name__ == '__main__':
    app.run(debug=True)