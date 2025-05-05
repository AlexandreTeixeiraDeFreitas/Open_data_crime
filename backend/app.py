from flask import Flask
from flask_cors import CORS

# Initialize the Flask application
app = Flask(__name__)

# Enable CORS for the application
CORS(app)

@app.route('/')
def home():
    return {"message": "Welcome to the Flask API with CORS enabled!"}

if __name__ == '__main__':
    app.run(debug=True)