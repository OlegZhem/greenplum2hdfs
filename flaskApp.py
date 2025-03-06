import numpy as np
from flask import Flask, render_template, request, jsonify
from src.ui_properties import settings, save_settings
from src.main import *

import logging
import sys
from pathlib import Path

# Create a formatter
_formatter = logging.Formatter('[%(asctime)s] %(message)s')
_formatter.default_msec_format = '%s.%03d'
# Create handlers
_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.setFormatter(_formatter)
_log_file = "logs/greenplum2hdfs.log"
_file_handler = logging.FileHandler(_log_file)
_file_handler.setFormatter(_formatter)
# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(_stdout_handler)
logger.addHandler(_file_handler)

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/pandas')
def pandas():
    return render_template('processing_pandas.html')


@app.route('/pandas/process', methods=['POST'])
def run_pandas_process():
    try:
        data = request.get_json()
        ds = data.get("data_source")
        dd = data.get("data_destination")

        # Convert string values to enum members.
        try:
            ds_enum = DataSource(ds)
            dd_enum = DataDestination(dd)
        except Exception as e:
            return jsonify({"error": "Invalid parameters provided."}), 400

        # Run the process function.
        process_pandas(ds_enum, dd_enum)

        return jsonify({"status": "Process completed successfully."})
    except ValueError:
        return jsonify({"error": "Invalid selection. Please choose valid options."}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


@app.route('/dask')
def dask():
    return render_template('processing_dask.html')


@app.route('/dask/process', methods=['POST'])
def run_dask_process():
    try:
        data = request.get_json()
        ds = data.get("data_source")
        dd = data.get("data_destination")

        # Convert string values to enum members.
        try:
            ds_enum = DataSource(ds)
            dd_enum = DataDestination(dd)
        except Exception as e:
            return jsonify({"error": "Invalid parameters provided."}), 400

        # Run the process function.
        process_dask(ds_enum, DataTransformerDask.FULL, dd_enum)

        return jsonify({"status": "Process completed successfully."})
    except ValueError:
        return jsonify({"error": "Invalid selection. Please choose valid options."}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


@app.route('/histogram')
def run_histogram():
    return render_template('histogram.html')


@app.route('/histogram/process', methods=['POST'])
def run_histogram_process():
    try:
        data = request.get_json()
        logger.info(f'{request.base_url} {data}')
        ds = data.get("data_source")
        column = data.get("data_column")
        bins = data.get("data_bins")

        # Convert string values.
        try:
            ds_enum = DataSource(ds)
            bins_int = int(bins)
        except Exception as e:
            logger.error("convert to DataSource", exc_info=e)
            return jsonify({"error": "Invalid parameters provided."}), 400

        # Run the process function.
        hist, bin_edges = create_histogram(ds_enum, column, bins_int)
        logger.debug(f'hist {type(hist)}: {hist}')
        logger.debug(f'bins {type(bin_edges)}: {bin_edges}')

        hist_np = hist.compute()  # Convert to NumPy array
        # Normalize the histogram (Convert counts to percentage)
        total_count = np.sum(hist_np)
        hist_normalized = (hist_np / total_count) * 100

        hist_list = hist_normalized.tolist()
        bins_list = bin_edges.tolist()
        logger.debug(f'hist list : {hist_list}')
        logger.debug(f'bins list: {bins_list}')

        return jsonify({
            "status": "Process completed successfully.",
            "hist": hist_list,
            "bins": bins_list
        })
    except ValueError:
        return jsonify({"error": "Invalid selection. Please choose valid options."}), 400
    except Exception as e:
        logger.error("convert to DataSource", exc_info=e)
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/conf_int')
def run_confidence_interval():
    return render_template('confidence_interval.html')


@app.route('/conf_int/calculate', methods=['POST'])
def get_moments_process():
    try:
        data = request.get_json()
        logger.info(f'{request.base_url} {data}')
        ds = data.get("data_source")
        column = data.get("data_column")

        # Convert string values.
        try:
            ds_enum = DataSource(ds)
        except Exception as e:
            logger.error("convert to DataSource", exc_info=e)
            return jsonify({"error": "Invalid parameters provided."}), 400

        mean, std, skew_val, kurtosis_val = get_moments(ds_enum, column)

        return jsonify({
            "status": "Process completed successfully.",
            "mean": mean,
            "std": std,
            "skew": skew_val,
            "kurtosis": kurtosis_val,
        })
    except ValueError:
        return jsonify({"error": "Invalid selection. Please choose valid options."}), 400
    except Exception as e:
        logger.error("convert to DataSource", exc_info=e)
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
