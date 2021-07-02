from wsgiref import simple_server

from flask import Flask, request, render_template
from flask import Response
from flask_cors import CORS, cross_origin

from trainValidationInsertion import TrainValidation

app = Flask(__name__)
# dashboard.bind(app)
CORS(app)


@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template("index.html")


@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():
    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            # path = "Training_Batch_Files"

            # intialsing object of train validation insertion and calling function
            train_val_obj = TrainValidation(path)
            train_val_obj.trainingDataValidation()


    except ValueError:

        return Response("Error Occurred! %s" % ValueError)


    except KeyError:

        return Response("Error Occurred! %s" % KeyError)


    except Exception as e:

        return Response("Error Occurred! %s" % e)

    return Response("Training successfull!!")


if __name__ == "__main__":
    app.run()
    host = '0.0.0.0'
    port = 5000
    httpd = simple_server.make_server(host, port, app)
    print("Serving on %s %d" % (host, port))
    httpd.serve_forever()
