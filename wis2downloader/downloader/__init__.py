from abc import ABC, abstractmethod
import urllib3
from urllib.parse import urlsplit
import hashlib
import base64
import os
import uuid
import subprocess
import glob
from datetime import datetime as dt
from pathlib import Path
import enum
import shutil
import json

from wis2downloader import stop_event
from wis2downloader.log import LOGGER
from wis2downloader.queue import BaseQueue
from wis2downloader.metrics import (DOWNLOADED_BYTES, DOWNLOADED_FILES,
                                    FAILED_DOWNLOADS)
from wis2downloader.tasks import process_downloaded_file
from wis2downloader.constants import map_media_type, VerificationMethods


class BaseDownloader(ABC):

    @abstractmethod
    def start(self):
        """Start the download worker to
        process messages from the queue indefinitely"""
        pass

    @abstractmethod
    def process_job(self, job):
        """Process a single job from the queue"""
        pass

    @abstractmethod
    def get_topic_and_centre(self, job):
        """Extract the topic and centre id from the job"""
        pass

    @abstractmethod
    def get_hash_info(self, job):
        """Extract the hash value and function from the job
        to be used for verification later"""
        pass

    @abstractmethod
    def get_download_url(self, job):
        """Extract the download url, update status, and
        file type from the job links"""
        pass

    @abstractmethod
    def extract_filename(self, _url):
        """Extract the filename and extension from the download link"""
        pass


class DownloadWorker(BaseDownloader):
    def __init__(self, queue: BaseQueue, basepath: str = ".", min_free_space=10):  # noqa
        timeout = urllib3.Timeout(connect=1.0)
        self.http = urllib3.PoolManager(timeout=timeout)
        self.queue = queue
        self.basepath = Path(basepath)
        self.min_free_space = min_free_space * 1073741824  # GBytes
        self.status = "ready"
        self.LAT_MIN = -100
        self.LAT_MAX = 15
        self.LON_MIN = -30
        self.LON_MAX = 60

    def start(self) -> None:
        LOGGER.info("Starting download worker")
        while not stop_event.is_set():
            # First get the job from the queue
            job = self.queue.dequeue()
            if job.get('shutdown', False):
                break

            self.status = "running"
            try:
                self.process_job(job)
            except Exception as e:
                LOGGER.error(e)

            self.status = "ready"
            self.queue.task_done()
            
    def check_coordinates(self, coordinates):
        """
        Check if the coordinates are within the defined bounds.
        """
        # Check if coordinates are a list of coordinates, or just a single coordinate
        # If it's a single coordinate, it should be a list of two elements [lat, lon]
        # if it's a list of coordinates, it should be a list of lists
        if isinstance(coordinates, list) and all(isinstance(coord, list) for coord in coordinates):
            # Check if any coordinate pair is within bounds
            return any(self.check_coordinates(coord) for coord in coordinates)
        # If it's a single coordinate, check if it's a list of two elements
        if not coordinates or not isinstance(coordinates, list) or len(coordinates) < 2:
            return False
        lat, lon = coordinates[0], coordinates[1]
        return (self.LAT_MIN <= lat <= self.LAT_MAX and
                self.LON_MIN <= lon <= self.LON_MAX)

    def get_free_space(self):
        total, used, free = shutil.disk_usage(self.basepath)
        return free

    def process_job(self, job) -> None:

        # Get information about the job for verification later
        expected_hash, hash_function_method = self.get_hash_info(job) # Renamed to avoid confusion with actual callable


        # Get the download url, update status, and file type from the job links
        _url, update, media_type, expected_size = self.get_download_url(job)

        if _url is None:
            LOGGER.warning(f"No download link found in job {job}")
            return

        # map media type to file extension
        file_type = map_media_type(media_type)

        # Global caches can set whatever filename they want, we need to use
        # the data_id for uniqueness. However, this can be unwieldy, hence use
        # hash of data_id
        data_id = job.get('payload', {}).get('properties', {}).get('data_id')
        
        # Get the coordinates from the job payload
        coordinates = job.get('payload', {}).get('geometry', [])
        coordinates = coordinates.get("coordinates", []) if isinstance(coordinates, dict) else coordinates
        LOGGER.info(f"Coordinates for job {data_id}: {coordinates}")
        
        # Check if the coordinates are within the defined bounds
        if not self.check_coordinates(coordinates):
            LOGGER.warning(f"Coordinates {coordinates} are out of bounds, skipping download for job {data_id}")
            return
        
        LOGGER.info(f"Coordinates {coordinates} are within bounds, proceeding with download for job {data_id}")  # noqa         
        
        # Get information needed for download metric labels
        topic, centre_id = self.get_topic_and_centre(job)
        
        # Prepare metadata to pass to the Celery task
        job_metadata = {
            'target': job.get('target', '.'),
            'expected_hash': expected_hash,
            'hash_method': hash_function_method, # Pass the method name, not the function object
            'expected_size': expected_size,
            'media_type': media_type,
            'topic': topic,
            'centre_id': centre_id,
            'data_id': data_id,
            'update': update, # Pass 'update' flag as well if needed in task
        }
        
        # Enqueue the task to Celery
        process_downloaded_file.delay(_url, job_metadata, str(self.basepath))

    def get_topic_and_centre(self, job) -> tuple:
        topic = job.get('topic')
        return topic, topic.split('/')[3]

    def get_hash_info(self, job):
        expected_hash = job.get('payload', {}).get(
            'properties', {}).get('integrity', {}).get('value')
        hash_method = job.get('payload', {}).get(
            'properties', {}).get('integrity', {}).get('method')

        return expected_hash, hash_method

    def get_download_url(self, job) -> tuple:
        links = job.get('payload', {}).get('links', [])
        _url = None
        update = False
        media_type = None
        expected_size = None
        for link in links:
            if link.get('rel') == 'update':
                _url = link.get('href')
                media_type = link.get('type')
                expected_size = link.get('length')
                update = True
                break
            elif link.get('rel') == 'canonical':
                _url = link.get('href')
                media_type = link.get('type')
                expected_size = link.get('length')
                break

        return _url, update, media_type, expected_size

    def extract_filename(self, _url) -> tuple:
        path = urlsplit(_url).path
        filename = os.path.basename(path)
        filename, filename_ext = os.path.splitext(filename)
        return filename, filename_ext