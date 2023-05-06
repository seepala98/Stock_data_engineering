from flask import Flask, request
import pickle

# Load the trained model from disk
model = pickle.load(open('./randomforest_model_stock_volume_prediction.sav', 'rb'))

app = Flask(__name__)

@app.route('/predict', methods=['GET'])
def predict():
    # Get the vol_moving_avg and adj_close_rolling_med values from the query parameters
    vol_moving_avg = float(request.args.get('vol_moving_avg'))
    adj_close_rolling_med = float(request.args.get('adj_close_rolling_med'))
    
    # Predict the trading volume using the loaded model
    prediction = int(model.predict([[vol_moving_avg, adj_close_rolling_med]]).item())
    
    # Return the prediction as a response
    return str(prediction)

if __name__ == '__main__':
    app.run(debug=True)
