from flask import Flask, render_template, request, jsonify
from src.ui_properties import settings, save_settings
from src.main import DataSource, DataTransformer, DataDestination, process

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/transformation')
def index():
    #return render_template('transformation.html')
    return render_template('transformation_bootstrap.html')


@app.route('/transformation/process', methods=['POST'])
def run_process():
    try:
        data = request.get_json()
        ds = data.get("data_source")
        dt = data.get("data_transformer")
        dd = data.get("data_destination")

        # Convert string values to enum members.
        try:
            ds_enum = DataSource(ds)
            dt_enum = DataTransformer(dt)
            dd_enum = DataDestination(dd)
        except Exception as e:
            return jsonify({"error": "Invalid parameters provided."}), 400

        # Run the process function.
        process(ds_enum, dt_enum, dd_enum)

        return jsonify({"status": "Process completed successfully."})
    except ValueError:
        return jsonify({"error": "Invalid selection. Please choose valid options."}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/settings', methods=['GET'])
def settings_page():
    return render_template('settings_bootstrap2.html', settings=settings)

@app.route('/settings/save', methods=['POST'])
def save_settings_route():
    global settings
    try:
        new_settings = request.get_json()
        save_settings(new_settings)  # Save to JSON
        settings.update(new_settings)  # Update in-memory settings
        return jsonify({"status": "Settings saved successfully."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
