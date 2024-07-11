from flask import Flask, jsonify
import json 

app = Flask(__name__)

@app.route('/data', methods=['GET'])
def get_data():
    data = []
    try:
        with open('/home/user/Desktop/kafka/data.json', 'r', encoding='utf-8') as f:
            for line in f:
                json_data = json.loads(line)
                data.append(json_data)
                print(json_data)  
    except FileNotFoundError:
        return jsonify({"error": "Data file not found"}), 404
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
