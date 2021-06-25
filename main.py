from wsgiref import simple_server
from flask import Flask,request,render_template
from flask import Response
import os 
from flask_cors import CORS, cross_origin
#from prediction_Validation_insertion import pred_validation 
#from trainingModel import trainModel
#import flask_monitoringDashboard as dashboard
#from predictModel import prediction 
import json



from trainValidationInsertion import TrainValidation





app = Flask(__name__)
#dashboard.bind(app)
#CORS(app)

@app.route("/",methods=['GET'])
#@cross_origin
def home():
    return render_template("index.html")


@app.route("/train",methods = ['GET'])
#@cross_origin
def trainRouteClient():
    try:
        path = "Training_Batch_Files"
      #  train_valObj = trainVa

if __name__ == "__main__":
    app.run()
   # host = '0.0.0.0'
   # port = 5000
   # httpd = simple_server.make_server(host, port, app)
    # print("Serving on %s %d" % (host, port))
   # httpd.serve_forever()
