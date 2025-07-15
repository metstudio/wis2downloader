import json
import subprocess
import tempfile
import pathlib
import urllib3
from celery import shared_task
from wis2downloader.log import LOGGER
from wis2downloader.utils.validate_celery_tasks import post_data

@shared_task(bind=True, name='process_bufr_data')
def process_bufr_data(self, bufr_content_bytes: bytes, data_id: str,
                      bufr2geojson_path: str | None,
                      post_config: dict | None,
                      save_geojson_locally: bool,
                      geojson_storage_path: str | None,
                      date: str | None = None):
    """
    A Celery task that can:
    1. Post raw BUFR data.
    2. Convert BUFR to GeoJSON.
    3. Save the resulting GeoJSON locally.
    4. Post the resulting GeoJSON to an API.
    The actions performed depend on the arguments received.
    """
    LOGGER.info(f"Celery task started for data_id: {data_id}")

    # Post the raw binary file if configured to do so ---
    if post_config and post_config.get("post_body_type") == "binary":
        LOGGER.info(f"Posting raw BUFR data for {data_id} as per configuration.")
        post_data(config=post_config, raw_bytes=bufr_content_bytes)

    # Convert to GeoJSON if needed for saving or posting ---
    # If we don't need to save or post GeoJSON, the task can end here.
    if not save_geojson_locally and (not post_config or post_config.get("post_body_type") != "json"):
        LOGGER.info(f"No GeoJSON action required for {data_id}. Task finished.")
        return

    if not bufr2geojson_path:
        LOGGER.error(f"Cannot process to GeoJSON for {data_id}: bufr2geojson_path was not provided.")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = pathlib.Path(temp_dir)
        try:
            LOGGER.info(f"Converting BUFR to GeoJSON for {data_id}...")
            subprocess.run(
                [bufr2geojson_path, 'data', 'transform', '-', '--output-dir', str(temp_dir_path)],
                input=bufr_content_bytes,
                capture_output=True,
                check=True
            )

            generated_files = list(temp_dir_path.glob('*.json'))
            if not generated_files:
                LOGGER.warning(f"bufr2geojson ran for {data_id} but produced no output files.")

            for json_file_path in generated_files:
                with open(json_file_path, 'r') as f:
                    geojson_data = json.load(f)

                # Save the GeoJSON file locally if requested ---
                if save_geojson_locally and geojson_storage_path:
                    storage_path = pathlib.Path(geojson_storage_path)
                    yyyy, mm, dd = date if date else (None, None, None)
                    if yyyy and mm and dd:
                        storage_path = storage_path / yyyy / mm / dd / data_id
                        storage_path.mkdir(parents=True, exist_ok=True)
                    else:
                        storage_path = storage_path / data_id
                        storage_path.mkdir(parents=True, exist_ok=True)
                    output_file = storage_path / json_file_path.name
                    try:
                        with open(output_file, 'w') as f_out:
                            json.dump(geojson_data, f_out, indent=4)
                        LOGGER.info(f"Saved GeoJSON for {data_id} to {output_file}")
                    except Exception as e:
                        LOGGER.error(f"Failed to save GeoJSON to {output_file}: {e}")

                # Post the GeoJSON data if requested ---
                if post_config and post_config.get("post_body_type") == "json":
                    LOGGER.info(f"Posting GeoJSON from {json_file_path.name} for {data_id}.")
                    post_data(config=post_config, json_payload=geojson_data)

        except subprocess.CalledProcessError as e:
            LOGGER.error(f"bufr2geojson failed for {data_id}: {e.stderr.decode('utf-8')}")
        except Exception as e:
            LOGGER.error(f"An unexpected error occurred in Celery task for {data_id}: {e}", exc_info=True)

    LOGGER.info(f"Celery task finished for data_id: {data_id}")
