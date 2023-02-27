import pymongo
import pandas as pd
from flask import Flask, render_template



def create_app():
    app = Flask(__name__, static_url_path='/static')


    @app.route('/', methods=["GET", "POST"]) 
    def welcome():
        """ 
        Our about us page.
        """
      return render_template('base.html')

    return app