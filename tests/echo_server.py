from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route('/', methods=['GET', 'PUT', 'POST', 'DELETE'])
def echo():
    resp = {'method':request.method,
            'data':request.data,
            'form':request.form,
            'args':request.args}
    
    return jsonify(resp)


if __name__ == '__main__':
    app.run()