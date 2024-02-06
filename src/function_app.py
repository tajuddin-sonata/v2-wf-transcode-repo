from json import dumps
from time import time
from typing import Union
from uuid import uuid1
start_time = time()
import logging
from pathlib import Path
import functions_framework
from flask import abort, g, make_response
from flask_expects_json import expects_json
from os import environ
import subprocess
from datetime import datetime, timedelta, timezone
import ffmpeg
from tempfile import TemporaryDirectory
# import tempfile
# import requests
# import json
# import io

# Azure Function Imports
import os
import sys
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from azure.functions import HttpRequest, HttpResponse
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from util_input_validation import schema, Config
from util_helpers import (
    impersonate_account,
    create_outgoing_file_ref,
    handle_bad_request,
    handle_exception,
    handle_not_found,
)

# Get the value of LD_LIBRARY_PATH
ld_library_path = os.environ.get("LD_LIBRARY_PATH")

AUDIO_WAVEFORM_PATH = "audiowaveform/audiowaveform"

# Instance-wide storage Vars
instance_id = str(uuid1())
run_counter = 0
connection_string = os.environ['StorageAccountConnectionString']
storage_client = BlobServiceClient.from_connection_string(connection_string)

time_cold_start = time() - start_time

temp_audio_format_name = "wav"
temp_audio_extension_type = ".wav"

waveform_extension = ".dat"

compressed_audio_format_name = "matroska"
compressed_audio_extension_type = ".mka"

app = func.FunctionApp()
@app.function_name(name="wf_transcode_HttpTrigger1")
@app.route(route="wf_transcode_HttpTrigger1")

def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    logging.info('HTTP trigger function processed a request.')

    ## context.function_directory returns the current directory in which functions is executed 
    waveform_liberary_path = os.path.join(context.function_directory, AUDIO_WAVEFORM_PATH)
    logging.info(f'liberary path: {waveform_liberary_path}')
    

    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    global run_counter
    run_counter += 1
    request_recieved = datetime.now(timezone.utc)
    request_json = req.get_json()
    CONFIG = Config(request_json)
    del request_json
    context = {
        **CONFIG.context.toJson(),
        "instance": instance_id,
        "instance_run": run_counter,
        "request_recieved": request_recieved.isoformat(),
    }
    logging.info(f'Received request: {context}')

    # Output Variables
    response_json = {}
    out_files = {}
    media_info = {
        "file_format": "",
        "video_streams": 0,
        "audio_streams": 0,
        "audio_channels": 0,
    }

    ### Get Media Blob
    media_blob = storage_client.get_container_client(
        CONFIG.input_files.media.bucket_name
    ).get_blob_client(
        CONFIG.input_files.media.full_path,
        # version_id=CONFIG.input_files.media.version,
    )
   
    try:
        ## Try to fetch blob properties with the condition that the ETag must match the desired_etag
        etag_value = media_blob.get_blob_properties(if_match=CONFIG.input_files.media.version)
        logging.info(f'Media Blob Name: {media_blob.blob_name}')
        logging.info(f'Media Blob ETag: {etag_value["etag"]}')

    except ResourceNotFoundError:
        ## Handle the case where the blob with the specified ETag is not found
        abort(404, "Media file not found on bucket")

    ## If blob exists, Generate Shared Access Signature (SAS) Token
    sas_token = generate_blob_sas(
                account_name=storage_client.account_name,
                account_key=storage_client.credential.account_key,
                container_name=CONFIG.input_files.media.bucket_name,
                blob_name=CONFIG.input_files.media.full_path,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(minutes=30),
            )

    ## Logging individual components of SAS token generation
    logging.info(f"Account Name: {storage_client.account_name}")
    logging.info(f"Container Name: {CONFIG.input_files.media.bucket_name}")
    logging.info(f"Blob Path: {CONFIG.input_files.media.full_path}")
    logging.info(f"Permission: {BlobSasPermissions(read=True)}")
    logging.info(f"Expiry: {datetime.utcnow() + timedelta(minutes=30)}")
    
    ## Log SAS token and request details
    logging.info(f"SAS Token: {sas_token}")
    logging.info(f"Request Headers: {dict(req.headers)}")

    blob_url = media_blob.url
    logging.info(f'Media Blob URL: {blob_url}')

    blob_filename = blob_url.split("/")[-1]
    logging.info(f'Blob filename: {blob_filename}')
    
    ## Combine the blob URL with the SAS token to get the signed URL
    staged_media_signed_url = f"{blob_url}?{sas_token}"

    ## Logging the staged_media_signed_url
    logging.info(f"Staged Media Signed URL: {staged_media_signed_url}")


    #####################################################
    # PROBE remote file for audio information
    ##################################################### 

    ##### check and Installing ffmpeg package #######
    try:
        ## Check if FFmpeg is already installed
        check_ffmpeg_installed = "ffmpeg -version"
        subprocess.run(check_ffmpeg_installed, shell=True, check=True)
        logging.info("FFmpeg is already installed.")
    except subprocess.CalledProcessError:
        ## If FFmpeg is not installed, attempt to install it
        install_command = "apt-get install -y ffmpeg || yum install -y ffmpeg"
        try:
            subprocess.run(install_command, shell=True, check=True)
            logging.info("FFmpeg installed successfully.")
        except subprocess.CalledProcessError as e:
            ## Log the exception details
            logging.exception("Error installing FFmpeg: %s", e)

    #############  PROBE remote file for audio information ####################
    try:
        ## Run ffmpeg.probe with the URL
        probe = ffmpeg.probe(staged_media_signed_url)
        media_info["file_format"] = probe["format"]["format_name"]
        media_info["video_streams"] = len(
            [stream for stream in probe["streams"] if stream["codec_type"] == "video"]
        )
        media_info["audio_streams"] = len(
            [stream for stream in probe["streams"] if stream["codec_type"] == "audio"]
        )
        if audio_stream := next(
            (stream for stream in probe["streams"] if stream["codec_type"] == "audio"),
            None
        ):
            media_info["audio_channels"] = audio_stream["channels"]

        ## Add success message
        logging.info("Probe successful! Media information: %s", media_info)
    except ffmpeg._run.Error as e:
        logging.error(dumps({"ffprobe_error": str(e.stderr, "utf-8")}, indent=2))
        abort(500, "ffprobe failed")
        

    #####################################################
    # HOTFIX: Add empty audio track to a video file if it has none
    # Upload as new working file
    #####################################################
    if media_info["audio_streams"] == 0 or media_info["audio_channels"] == 0:
        with TemporaryDirectory() as tmpdir:
            ## Add empty audio tracks to the file
            local_media_path = (
                Path(tmpdir, "local_media")
                .with_suffix(Path(CONFIG.input_files.media.full_path).suffix)
                .as_posix()
            )
            try:
                ffmpeg.output(
                    ffmpeg.input(staged_media_signed_url), 
                    ffmpeg.input("anullsrc",format='lavfi'), 
                    local_media_path, 
                    shortest=None
                ).run(quiet=True)

            except ffmpeg._run.Error as e:
                logging.error(dumps({"ffprobe_error": str(e.stderr, "utf-8")}, indent=2))
                abort(500, "injecting blank audio into video transcoding failed")

            ## Upload the new file, replacing the existing one on the Staging Bucket
            staging_video_with_added_blank_audio_path = (
            Path(
                str(CONFIG.staging_config.folder_path),
                str(CONFIG.staging_config.file_prefix) + "_" + "video_with_added_blank_audio",
            )
            .with_suffix(Path(CONFIG.input_files.media.full_path).suffix)
            .as_posix()
            )

            video_with_added_blank_audio_blob = storage_client.get_container_client(
                CONFIG.staging_config.bucket_name
            ).get_blob_client(staging_video_with_added_blank_audio_path)

            ## video_with_added_blank_audio_blob.upload_from_filename(local_media_path, timeout=300)
            
            with open(local_media_path, "rb") as f:
                video_with_added_blank_audio_blob.upload_blob(f, timeout=300, overwrite=True)

        ## Check that a new generation of the media_blob exists now
        if video_with_added_blank_audio_blob.get_blob_properties().etag == CONFIG.input_files.media.version:
            logging.warning(
                "media file was not overwritten properly with new empty audio version"
            )
        else:
            out_files["video_with_added_blank_audio"] = create_outgoing_file_ref(video_with_added_blank_audio_blob)
        
        ## Swap out the staged media URL with a new one
        sas_token = generate_blob_sas(
                account_name=storage_client.account_name,
                account_key=storage_client.credential.account_key,
                container_name=CONFIG.staging_config.bucket_name,
                blob_name=staging_video_with_added_blank_audio_path,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(minutes=30),
            )
        staged_media_signed_url = f"{video_with_added_blank_audio_blob.url}?{sas_token}"
        
        ## Logging the staged_media_signed_url
        logging.info(f"Staged Media Signed URL: {staged_media_signed_url}")

    #####################################################
    # Convert to WAV if not already WAV
    #####################################################
    waveform_input_bytes = None
    if temp_audio_format_name not in media_info["file_format"].split(","):
        with TemporaryDirectory() as tmpdir:
            local_temp_audio_path = (
                Path(tmpdir, "temp_audio")
                .with_suffix(temp_audio_extension_type)
                .as_posix()
            )
            try:
                ffmpeg.input(staged_media_signed_url).output(local_temp_audio_path).run(        
                    quiet=True
                )
            except ffmpeg._run.Error as e:
                logging.error(dumps({"ffprobe_error": str(e.stderr, "utf-8")}, indent=2))
                abort(500, "temp audio wav transcoding failed")

            # DO we need to store this for transcription later?
            # If file was VIDEO, upload temp audio to bucket and return it
            if media_info["video_streams"] > 0:
                staging_temp_audio_path = (
                    Path(
                        str(CONFIG.staging_config.folder_path),
                        str(CONFIG.staging_config.file_prefix) + "_" + "temp_audio",
                    )
                    .with_suffix(temp_audio_extension_type)
                    .as_posix()
                )
                temp_audio_blob = storage_client.get_container_client(
                    CONFIG.staging_config.bucket_name
                ).get_blob_client(staging_temp_audio_path)
                # temp_audio_blob.upload_from_filename(local_temp_audio_path, timeout=300)
                
                with open(local_temp_audio_path, "rb") as f:
                    temp_audio_blob.upload_blob(f, timeout=300, overwrite=True)
                
                if not temp_audio_blob.exists():
                    abort(500, "temp audio failed to upload to bucket")
                out_files["temp_audio"] = create_outgoing_file_ref(temp_audio_blob)

            # Now Grab the local file as Bytes
            with open(local_temp_audio_path, "rb") as local_temp_audio:
                waveform_input_bytes = local_temp_audio.read()
    else:
        # if the original file is already the right format,
        # just grab the file as bytes without converting
        waveform_input_bytes = media_blob.download_blob().readall()
        # logging.info(f"waveform input bytes: {waveform_input_bytes}")

    #####################################################
    # Generate Waveform
    #####################################################
        
    with TemporaryDirectory() as tmpdir:
        local_waveform_file_path = (
            Path(tmpdir, "waveform").with_suffix(waveform_extension).as_posix()
        )
        try:
            subprocess.check_output(
                [
                    waveform_liberary_path,
                    "--input-format",
                    "wav",
                    "-o",
                    local_waveform_file_path,
                    "-b",
                    "8",
                ]
                + (["--split-channels"] if media_info["audio_channels"] > 1 else []),
                input=waveform_input_bytes,
            )
            logging.info("Waveform generation successful. Local file path: %s", local_waveform_file_path)  #######logging the output
        
        except subprocess.CalledProcessError as e:
            logging.error(
                dumps(
                    {"bbcaudiowaveform_error": str(e) + ": " + str(e.stderr)}, indent=2
                )
            )
            abort(500, "waveform generation failed")
        del waveform_input_bytes
        # Create Waveform BLOB
        staging_waveform_audio_path = (
            Path(
                str(CONFIG.staging_config.folder_path),
                str(CONFIG.staging_config.file_prefix) + "_" + "waveform",
            )
            .with_suffix(waveform_extension)
            .as_posix()
        )
        waveform_file_blob = storage_client.get_container_client(
            CONFIG.staging_config.bucket_name
        ).get_blob_client(staging_waveform_audio_path)
        # Upload Waveform
        # waveform_file_blob.upload_from_filename(local_waveform_file_path, timeout=300)

        with open(local_waveform_file_path, "rb") as f:
            waveform_file_blob.upload_blob(f, timeout=300, overwrite=True)

    if not waveform_file_blob.exists():
        abort(500, "waveform failed to upload to bucket")
    out_files["waveform"] = create_outgoing_file_ref(waveform_file_blob)

    #####################################################
    ## Compress Audio to MKA
    #####################################################
    with TemporaryDirectory() as tmpdir:
        local_compressed_audio_path = (
            Path(tmpdir, "compressed_audio")
            .with_suffix(compressed_audio_extension_type)
            .as_posix()
        )
        try:
            ffmpeg.input(staged_media_signed_url).output(
                local_compressed_audio_path
            ).run(quiet=True)

        except Exception as e:
            logging.error(dumps({"ffprobe_error": str(e)}, indent=2))
            abort(
                500,
                "{} transcoding failed".format(
                    compressed_audio_extension_type.replace(".", "")
                ),
            )

        # Upload waveform and mka to staging bucket
        # Create BLOB
        staging_compressed_audio_path = (
            Path(
                str(CONFIG.staging_config.folder_path),
                str(CONFIG.staging_config.file_prefix) + "_" + "compressed_audio",
            )
            .with_suffix(compressed_audio_extension_type)
            .as_posix()
        )
        compressed_audio_blob = storage_client.get_container_client(
            CONFIG.staging_config.bucket_name
        ).get_blob_client(staging_compressed_audio_path)
        
        #### Upload
        # compressed_audio_blob.upload_from_filename(local_compressed_audio_path, timeout=300)
        #### Upload
        with open(local_compressed_audio_path, "rb") as f:
            compressed_audio_blob.upload_blob(f, timeout=300, overwrite=True)
    
    if not compressed_audio_blob.exists():
        abort(500, "compressed audio failed to upload to bucket")
    out_files["compressed_audio"] = create_outgoing_file_ref(compressed_audio_blob)
    # Return file info and uploaded version numbers

    response_json["status"] = "success"
    response_json["staged_files"] = out_files
    response_json["media_info"] = media_info
    # return make_response(response_json, 200)
    return func.HttpResponse(body=dumps(response_json), status_code=200, mimetype='application/json')