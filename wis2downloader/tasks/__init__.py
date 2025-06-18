import os
import shutil
import subprocess
import glob
import uuid
import base64
import hashlib
from datetime import datetime as dt
from pathlib import Path
import urllib3
from urllib.parse import urlsplit

from celery import Celery
from wis2downloader.celery import app

from wis2downloader.log import LOGGER
from wis2downloader.metrics import (DOWNLOADED_BYTES, DOWNLOADED_FILES,
                                    FAILED_DOWNLOADS)
from wis2downloader.constants import VerificationMethods, map_media_type

# Initialize urllib3 PoolManager outside the task for efficiency if possible,
# or create it inside if each task needs its own connection.
# For simplicity, creating inside for now, but consider connection pooling for heavy usage.

@app.task(bind=True, max_retries=3, default_retry_delay=300) # retry after 5 minutes
def process_downloaded_file(self, job_url: str, job_metadata: dict, basepath_str: str):
    """
    Celery task to process a downloaded file.
    Takes the download URL and job metadata as input.
    """
    LOGGER.info(f"Celery task received job for URL: {job_url}")

    http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=1.0))
    basepath = Path(basepath_str)

    try:
        # Reconstruct necessary info from job_metadata
        yyyy, mm, dd = dt.now().year, dt.now().month, dt.now().day
        output_dir = basepath / f"{yyyy:04}" / f"{mm:02}" / f"{dd:02}"
        output_dir = output_dir / job_metadata.get("target", ".")

        expected_hash = job_metadata.get('expected_hash')
        hash_method = job_metadata.get('hash_method')
        expected_size = job_metadata.get('expected_size')
        media_type = job_metadata.get('media_type')
        topic = job_metadata.get('topic')
        centre_id = job_metadata.get('centre_id')
        data_id = job_metadata.get('data_id')

        file_type = map_media_type(media_type)

        # Re-extract filename
        path = urlsplit(job_url).path
        filename_base = os.path.basename(path)
        filename_base, _ = os.path.splitext(filename_base)
        filename = filename_base + '.' + file_type
        target = output_dir / filename

        # Create parent dir if it doesn't exist
        target.parent.mkdir(parents=True, exist_ok=True)

        # Download the file
        download_start = dt.now()
        response = None
        try:
            response = http.request('GET', job_url)
            if response.status != 200:
                LOGGER.error(f"Error downloading {job_url} within Celery task, received status code: {response.status}")
                FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
                raise Exception(f"HTTP error: {response.status}")
            filesize = len(response.data)
        except Exception as e:
            LOGGER.error(f"Error downloading {job_url} in Celery task: {e}")
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            raise # Re-raise to allow Celery to retry if configured

        # Re-validate data (can be moved to a helper function if desired)
        hash_function = None
        if hash_method in VerificationMethods._member_names_:
            method = VerificationMethods[hash_method].value
            hash_function = getattr(hashlib, method, None)

        save_data = True
        if expected_hash and hash_function and expected_size is not None:
            try:
                hash_value = hash_function(response.data).digest()
                hash_value = base64.b64encode(hash_value).decode()
            except Exception as e:
                LOGGER.error(f"Error calculating hash in Celery task: {e}")
                save_data = False
            if save_data and ((hash_value != expected_hash) or (len(response.data) != expected_size)):
                save_data = False

        if not save_data:
            LOGGER.warning(f"Download {data_id} failed verification in Celery task, discarding")
            FAILED_DOWNLOADS.labels(topic=topic, centre_id=centre_id).inc(1)
            return

        # Save the file
        try:
            target.write_bytes(response.data)
            download_end = dt.now()
            download_time = download_end - download_start
            download_seconds = round(download_time.total_seconds(), 2)
            LOGGER.info(
                f"Downloaded {filename} of size {filesize} bytes in {download_seconds} seconds within Celery task"
            )

            # Increment metrics
            all_type_labels = ['bufr', 'grib', 'json', 'xml', 'png']
            file_type_label = 'other'
            for label in all_type_labels:
                if label in file_type:
                    file_type_label = label
                    break

            DOWNLOADED_BYTES.labels(
                topic=topic, centre_id=centre_id,
                file_type=file_type_label).inc(filesize)
            DOWNLOADED_FILES.labels(
                topic=topic, centre_id=centre_id,
                file_type=file_type_label).inc(1)

        except Exception as e:
            LOGGER.error(f"Error saving to disk in Celery task: {target}: {e}")
            raise # Re-raise to allow Celery to retry

        # BUFR to GeoJSON conversion and Django management command processing
        if filename.endswith('.bufr') or filename.endswith('.bin'):
            LOGGER.info(f"Detected BUFR file: {filename}. Converting to GeoJSON and processing...")

            python_venv_path = '/home/rimes/observation/venv/bin/python'
            bufr2geojson_path = '/home/rimes/observation/venv/bin/bufr2geojson'
            manage_py_path = '/home/rimes/observation/manage.py'

            unique_id = str(uuid.uuid4())[:8]
            temp_dir = basepath / f"output/temp_json_files_{unique_id}"
            temp_dir.mkdir(parents=True, exist_ok=True)

            try:
                cmd = [bufr2geojson_path, 'data', 'transform', str(target), '--output-dir', str(temp_dir)]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                LOGGER.info(f"Converted BUFR to GeoJSON: {result.stdout}")

                json_files = glob.glob(str(temp_dir / '*.json'))
                LOGGER.info(f"Found {len(json_files)} GeoJSON files to process.")

                success_count = 0
                for json_file in json_files:
                    LOGGER.info(f"Processing GeoJSON file: {os.path.basename(json_file)}")
                    try:
                        result = subprocess.run(
                            [python_venv_path, manage_py_path, 'update_obs_data', json_file],
                            capture_output=True,
                            text=True,
                            check=True,
                        )
                        if "out of bounds" in result.stdout.lower():
                            LOGGER.info(result.stdout)
                            LOGGER.info("All GeoJSON files likely out of bounds. Aborting processing and cleaning up.")
                            break
                        LOGGER.info(f"Successfully processed {os.path.basename(json_file)}")
                        LOGGER.info(result.stdout)
                        success_count += 1
                    except subprocess.CalledProcessError as e:
                        error_message = e.stderr + e.stdout
                        if "out of bounds" in error_message.lower():
                            LOGGER.info(f"Skipped {os.path.basename(json_file)}: Station is out of bounds")
                            LOGGER.info("All GeoJSON files likely out of bounds. Aborting processing and cleaning up.")
                            break
                        else:
                            LOGGER.error(f"Error processing {os.path.basename(json_file)}: {e}")
                            LOGGER.error(f"Command: {e.cmd}")
                            LOGGER.error(f"Exit Code: {e.returncode}")
                            LOGGER.error(f"Stderr: {e.stderr}")
                            LOGGER.error(f"Stdout: {e.stdout}")

                LOGGER.info(
                    f"Completed processing. Successfully processed {success_count} out of {len(json_files)} GeoJSON files."
                )

            except subprocess.CalledProcessError as e:
                LOGGER.error(f"Error converting BUFR file {filename} in Celery task: {e}")
                LOGGER.error(e.stderr)
                raise # Re-raise to allow Celery to retry

            finally:
                try:
                    shutil.rmtree(temp_dir)
                    LOGGER.info(f"Removed temporary directory {temp_dir} in Celery task")
                except OSError as e:
                    LOGGER.warning(f"Error removing directory {temp_dir} in Celery task: {e}")

    except Exception as e:
        LOGGER.error(f"An unexpected error occurred in Celery task for URL {job_url}: {e}")
        self.retry(exc=e) # Celery will retry the task