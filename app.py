from flask import Flask, jsonify, request, Blueprint
from data_manneger.queries_2 import connection_queries
from data_manneger.queries import stat_queries
from data_manneger.crud_service import crud_bp

app = Flask(__name__)
app.register_blueprint(connection_queries, url_prefix='/api')
app.register_blueprint(stat_queries, url_prefix='/api')
app.register_blueprint(crud_bp, url_prefix='/api')




if __name__ == '__main__':
    app.run(debug=True, port=5000)

