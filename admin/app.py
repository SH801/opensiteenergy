# ***********************************************************
# ********************* OPEN WIND ENERGY ********************
# ***********************************************************
# ******************* Server admin script *******************
# ***********************************************************
# ***********************************************************
# v1.0

# ***********************************************************
#
# MIT License
#
# Copyright (c) Stefan Haselwimmer, OpenSite.energy, 2026
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys, os
sys.path.insert(0, os.getcwd())

import uuid
import requests
import socket
import validators
import shutil
import secrets
import subprocess
import json
import time
import os
import yaml
import psycopg2
import zipfile
from pathlib import Path
from bs4 import BeautifulSoup
from requests import get
from datetime import datetime
from io import BytesIO
from pathlib import Path
from psycopg2 import sql
from psycopg2.extensions import AsIs
from flask import Flask, session, render_template, request, redirect, url_for, send_file, Response, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from os import listdir
from os.path import isfile, isdir, basename, join

load_dotenv("../.env")

WORKING_FOLDER                      = Path(__file__).absolute().parent
BUILD_FOLDER                        = Path(WORKING_FOLDER).parent / 'build'
if os.environ.get("BUILD_FOLDER") is not None: 
    BUILD_FOLDER = Path(os.environ.get('BUILD_FOLDER'))
BUILD_CONFIGS_FOLDER                = BUILD_FOLDER / 'configs'

POSTGRES_HOST                       = os.environ.get("POSTGRES_HOST")
POSTGRES_DB                         = os.environ.get("POSTGRES_DB")
POSTGRES_USER                       = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD                   = os.environ.get("POSTGRES_PASSWORD")
PROCESSING_COMMAND_LINE_EXTERNAL    = './build-cli.sh'
PROCESSING_COMMAND_LINE_SERVER      = '../build-server.sh'
PROCESSING_STATE_FILE               = '../PROCESSING'
PROCESSING_START                    = '../PROCESSINGSTART'
PROCESSING_COMPLETE                 = '../PROCESSINGCOMPLETE'
CERTBOT_LOG                         = '../log-certbot.txt'
PROCESSING_ENGINE_URL               = "http://127.0.0.1:8000"

app = Flask(__name__)
CORS(app)
application = app

# ***********************************************************
# ***************** General helper functions ****************
# ***********************************************************

def addSecretKeyToEnv(env_path):
    """
    Adds secret key to environment file if it doesn't have one already
    """

    secret_key = secrets.token_urlsafe(30)
    with open(env_path, 'a', encoding='utf-8') as file: 
        file.write("SECRET_KEY=" + str(secret_key) + "\n")

def getFilesInFolder(folderpath):
    """
    Get list of all files in folder
    Create folder if it doesn't exist
    """

    files = [f for f in listdir(folderpath) if ((f != '.DS_Store') and (isfile(join(folderpath, f))))]
    if files is not None: files.sort()
    return files

def buildProcessingCommandLine(build_parameters):
    """
    Builds processing command line leaving out empty parameters where appropriate
    """

    global PROCESSING_COMMAND_LINE_EXTERNAL, PROCESSING_COMMAND_LINE_SERVER

    command_line_parameters = ''

    if build_parameters['height_to_tip'] != '': 
        command_line_parameters += ' ' + str(build_parameters['height_to_tip'])
        if build_parameters['blade_radius'] != '':
            command_line_parameters += ' ' + str(build_parameters['blade_radius'])

    if build_parameters['clip'] != '': 
        command_line_parameters += " --clip '" + str(build_parameters['clip']) + "'"

    if build_parameters['custom_configuration'] != '': 
        command_line_parameters += " --custom '" + str(build_parameters['custom_configuration']) + "'"

    if build_parameters['purge_all']:
        command_line_parameters += " --purgeall"

    session['command_line'] = PROCESSING_COMMAND_LINE_EXTERNAL + command_line_parameters
    session['command_line_server'] = PROCESSING_COMMAND_LINE_SERVER + command_line_parameters

    return session['command_line']

def isProcessing():
    """
    Gets current state of processing queue using processing file flag
    """

    global PROCESSING_STATE_FILE

    return isfile(PROCESSING_STATE_FILE)

def startOpenSiteEnergy():
    """
    Starts Open Site Energy service using systemd opensiteenergy-servicesmanager
    """

    if isfile('/usr/src/opensiteenergy/OPENWINDENERGY-START'): return
    with open('/usr/src/opensiteenergy/OPENWINDENERGY-START', 'w') as file: file.write("OPENWINDENERGY-START")
    while True:
        if not isfile('/usr/src/opensiteenergy/OPENWINDENERGY-START'): break
        time.sleep(0.5)

def stopOpenSiteEnergy():
    """
    Stops Open Site Energy service using systemd opensiteenergy-servicesmanager
    """

    if isfile('/usr/src/opensiteenergy/OPENWINDENERGY-STOP'): return
    with open('/usr/src/opensiteenergy/OPENWINDENERGY-STOP', 'w') as file: file.write("OPENWINDENERGY-STOP")
    while True:
        if not isfile('/usr/src/opensiteenergy/OPENWINDENERGY-STOP'): break
        time.sleep(0.5)

def setProcessing(processing_state, command_line=''):
    """
    Sets processing state
    """

    global PROCESSING_STATE_FILE, PROCESSING_COMPLETE, PROCESSING_COMMAND_LINE_SERVER

    if isfile(PROCESSING_COMPLETE): os.remove(PROCESSING_COMPLETE)

    if processing_state:
        with open(PROCESSING_STATE_FILE, 'w', encoding='utf-8') as file: file.write(command_line)
        # Restart processing system daemon using command line parameters

        stopOpenSiteEnergy()
        with open(PROCESSING_COMMAND_LINE_SERVER, 'w', encoding='utf-8') as file: 
            file.write("""#!/bin/bash
""" + command_line + """
if ! [ -f "PROCESSING" ]; then
    ./build-tileserver-gl.sh
fi
""")
        startOpenSiteEnergy()
    else:
        # Stop processing system daemon and reset build-server.sh to default state - in case machine gets restarted
        stopOpenSiteEnergy()
        if isfile(PROCESSING_STATE_FILE): os.remove(PROCESSING_STATE_FILE)
        with open(PROCESSING_COMMAND_LINE_SERVER, 'w', encoding='utf-8') as file: 
            file.write("""#!/bin/bash

./build-cli.sh

# ****************************************************************
# ***** Perform post-build setup specific to server install ******
# ****************************************************************
# Check that processing has finished (PROCESSING has been deleted) then:
# - Set up tileserver-gl as service
# - Change Apache2 settings to point to new tiles
# - Restart tileserver-gl and Apache2

if ! [ -f "PROCESSING" ]; then
    ./build-tileserver-gl.sh
fi

""")

def isLoggedIn():
    """
    Gets user logged in state
    """

    if 'logged_in' not in session: session['logged_in'] = False
    return session['logged_in']

def error(errortext):
    """
    Generic error page
    """

    return render_template("error.html", errortext=errortext) 

def runSubprocess(subprocess_array):
    """
    Runs subprocess
    """

    if subprocess_array[0] == 'ogr2ogr': subprocess_array.append('-progress')

    output = subprocess.run(subprocess_array)

    if output.returncode != 0: print("subprocess.run failed with error code: " + str(output.returncode) + '\n' + " ".join(subprocess_array))
    return " ".join(subprocess_array)

# ***********************************************************
# ******************** PostGIS functions ********************
# ***********************************************************

def postgisCheckTableExists(table_name):
    """
    Checks whether table already exists
    """

    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    table_name = table_name.replace("-", "_")
    conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    cur = conn.cursor()
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s);", (table_name, ))
    tableexists = cur.fetchone()[0]
    cur.close()
    return tableexists

def postgisGetResults(sql_text, sql_parameters=None):
    """
    Runs database query and returns results
    """

    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    cur = conn.cursor()
    cur.execute(sql_text, sql_parameters)
    results = cur.fetchall()
    conn.close()
    return results

def postgisGetClippingAreas():
    """
    Gets all available clipping areas from PostGIS _opensite_clipping_master table
    """

    if not postgisCheckTableExists('_opensite_osm_boundaries'): 
        print("Table missing")
        return []
    
    clippingareas = postgisGetResults("""
    SELECT DISTINCT all_names.name FROM 
    (
        SELECT DISTINCT name name FROM _opensite_osm_boundaries WHERE name <> '' AND admin_level <> '4' UNION 
        SELECT DISTINCT council_name name FROM _opensite_osm_boundaries WHERE council_name <> '' AND admin_level <> '4' 
    ) all_names ORDER BY all_names.name;""")
    clippingareas = [clippingarea[0] for clippingarea in clippingareas]

    return clippingareas

# ***********************************************************
# ************** Application logic functions ****************
# ***********************************************************

@app.route("/")
def home(): 
    """
    Website home page - return the web app index page
    """

    return render_template("index.html")

@app.route('/ckan')
def proxy():
    target_url = request.args.get('url')
    if not target_url:
        return "No URL provided", 400

    try:
        # 1. Use a real User-Agent (CKAN sometimes blocks 'python-requests')
        # 2. verify=False can be used if you have local SSL certificate issues
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(target_url, headers=headers, stream=True, timeout=15)
        
        # 3. Check if CKAN actually returned the file
        response.raise_for_status()

        # 4. Return the content with the original content-type (e.g., text/yaml)
        return Response(
            response.content,
            status=response.status_code,
            content_type=response.headers.get('Content-Type')
        )

    except requests.exceptions.RequestException as e:
        print(f"CRITICAL PROXY ERROR: {e}")
        return f"Proxy failed to reach CKAN: {str(e)}", 502
    except Exception as e:
        print(f"INTERNAL SERVER ERROR: {e}")
        return f"Internal Error: {str(e)}", 500



@app.route("/admin")
def admin():
    """
    Admin home page
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    if isProcessing(): return redirect(url_for('settings'))

    return redirect(url_for('settings'))

@app.route("/login")
def login():
    """
    Show login page
    """

    session['logged_in'] = False

    error = ''
    if 'error' in session: error = session['error']
    session['error'] = ''

    return render_template("login.html", error=error) 

@app.route("/logout")
def logout():
    """
    Logs user out
    """

    session['logged_in'] = False

    return redirect(url_for('admin'))

@app.route("/processlogin", methods=['POST'])
def processlogin():
    """
    Process login credentials
    """

    session['logged_in'] = False

    server_envvars = "../.env-server"
    if not isfile(server_envvars):
        return error("Server login credentials file missing")

    load_dotenv(server_envvars)

    if ('SERVER_USERNAME' not in os.environ) or ('SERVER_PASSWORD' not in os.environ):
        return error("Server credentials file missing username or password")

    username = request.form.get('username', '').strip()
    password = request.form.get('password', '').strip()

    server_username = os.environ['SERVER_USERNAME']
    server_password = os.environ['SERVER_PASSWORD']
    
    time.sleep(5)

    if (username != server_username) or (password != server_password): 
        session['error'] = 'Login failed'
        return redirect(url_for('login'))

    session['logged_in'] = True

    return redirect(url_for('settings'))

@app.route("/settings") 
def settings():
    """
    Renders settings page
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    # clippingareas = postgisGetClippingAreas()

    # return render_template("settings.html", clippingareas=clippingareas) 

    return render_template("settings.html") 

@app.route("/files") 
def files():
    """
    Renders downloadable files page
    """

    global BUILD_FOLDER

    if not isLoggedIn(): return redirect(url_for('login'))

    output_files_folder = BUILD_FOLDER / 'output' / 'layers'
    if output_files_folder.is_dir():
        output_files = getFilesInFolder(str(output_files_folder))
        files = [{'name': basename(file), 'url': '/outputfiles/' + file} for file in output_files]
    else:
        files=[]

    qgis_file = output_files_folder.parent / 'windconstraints--latest.qgs'
    qgis = isfile(str(qgis_file))

    return render_template("files.html", files=files, qgis=qgis) 

def download(zip_suffix, filter):
    """
    Generic download function
    """

    output_files_folder = BUILD_FOLDER / 'output' / 'layers'
    output_files = getFilesInFolder(str(output_files_folder))

    memory_file = BytesIO()
    with zipfile.ZipFile(memory_file, 'w') as zf:
        for output_file in output_files:
            if filter is not None:
                file_extension = output_file.split('.')[1]
                if file_extension not in filter: continue
            output_file = output_files_folder / output_file 
            zf.write(str(output_file), arcname=output_file.name)
    memory_file.seek(0)
    return send_file(memory_file, download_name=('opensiteenergy-' + zip_suffix + '.zip'), as_attachment=True)

@app.route("/downloadall") 
def downloadall():
    """
    Zips all files and returns to user
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    return download('files-all', None)

@app.route("/downloadgpkg") 
def downloadgpkg():
    """
    Zips all GPKG files and returns to user
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    return download('files-gpkg', ['gpkg'])

@app.route("/downloadgeojson") 
def downloadgeojson():
    """
    Zips all GeoJSON files and returns to user
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    return download('files-geojson', ['geojson'])

@app.route("/downloadshp") 
def downloadshp():
    """
    Zips all shp files and returns to user
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    return download('files-shp', ['shp', 'prj', 'shx', 'dbf'])

@app.route("/downloadqgis") 
def downloadqgis():
    """
    Zips QGIS file and all latest GPKG files and returns to user
    """

    # *** NEEDS FIXING ***
    global BUILD_FOLDER

    if not isLoggedIn(): return redirect(url_for('login'))

    output_files_folder = BUILD_FOLDER / 'output' / 'layers'
    output_files = getFilesInFolder(str(output_files_folder))

    memory_file = BytesIO()
    with zipfile.ZipFile(memory_file, 'w') as zf:
        qgis_file = join(output_files_folder, "..", "windconstraints--latest.qgs")
        if not isfile(qgis_file): return render_template('error.html', "No QGIS file has been created yet.")
        zf.write(qgis_file, arcname=basename(qgis_file))
        for output_file in output_files:
            if not output_file.startswith('latest--'): continue
            if not output_file.endswith('.gpkg'): continue
            output_file = join(output_files_folder, output_file) 
            zf.write(output_file, arcname=("output/" + basename(output_file)))
    memory_file.seek(0)
    return send_file(memory_file, download_name=('opensiteenergy-qgis.zip'), as_attachment=True)

@app.route("/serverlogs")
def serverlogs():
    """
    Renders logs page
    """

    global PROCESSING_START, PROCESSING_STATE_FILE

    if not isLoggedIn(): return redirect(url_for('login'))
    if not isProcessing(): return redirect(url_for('settings'))

    if 'command_line' not in session: session['command_line'] = ''

    if isfile(PROCESSING_STATE_FILE):
        with open(PROCESSING_STATE_FILE, "r", encoding='utf-8') as text_file: session['command_line'] = text_file.read().strip()

    started_datetime = None
    if isfile(PROCESSING_START):
        with open(PROCESSING_START, "r", encoding='utf-8') as text_file: started_datetime = text_file.read().strip()

    # Only show 'Stop processing' button during a build - if software install, hide it
    hide_stop = False
    if 'build-cli.sh' not in session['command_line']: hide_stop = True

    # Pass along headers, body, and query params
    resp = requests.request(
        method=request.method,
        url=PROCESSING_ENGINE_URL,
        headers={k: v for k, v in request.headers if k.lower() != 'host'},
        data=request.get_data(),
        params=request.args,
        cookies=request.cookies,
        allow_redirects=False
    )

    # Exclude hop-by-hop headers
    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    headers = [(name, value) for (name, value) in resp.raw.headers.items()
               if name.lower() not in excluded_headers]

    # return Response(resp.content, resp.status_code, headers)

        
    soup = BeautifulSoup(resp.text, 'html.parser')
    styles = "".join([str(s) for s in soup.find_all('style')])        
    body_content = "".join([str(item) for item in soup.body.contents])
        
    return render_template("processmonitor.html", styles=styles, body_content=body_content) 

@app.route("/nodes")
def nodes():
    """
    Renders retrieves node data via processing proxy
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    # Pass along headers, body, and query params
    resp = requests.request(
        method=request.method,
        url=f"{PROCESSING_ENGINE_URL}/nodes",
        headers={k: v for k, v in request.headers if k.lower() != 'host'},
        data=request.get_data(),
        params=request.args,
        cookies=request.cookies,
        allow_redirects=False
    )

    # Exclude hop-by-hop headers
    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    headers = [(name, value) for (name, value) in resp.raw.headers.items()
               if name.lower() not in excluded_headers]

    return Response(resp.content, resp.status_code, headers)

@app.route("/processingstart", methods=['POST'])
def processingstart():
    """
    Start processing
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    height_to_tip = request.form.get('height-to-tip', '')
    blade_radius = request.form.get('blade-radius', '')
    clip = request.form.get('clip', '').strip()
    custom_configuration = request.form.get('custom-configuration', '').strip()
    purge_all = request.form.get('purge-all', False)

    inputs_invalid = False

    if height_to_tip != '':
        try:
            test_float = float(height_to_tip)
        except:
            inputs_invalid = True

    if blade_radius != '':
        try:
            test_float = float(blade_radius)
        except:
            inputs_invalid = True

    if custom_configuration != '':
        if not validators.url(custom_configuration): custom_configuration = ''
        custom_configuration = custom_configuration.replace("'", "")

    clippingareas = postgisGetClippingAreas()
    clippingareas += ["England", "Cymru / Wales", "Alba / Scotland", "Northern Ireland / Tuaisceart Éireann"]

    if clip != '':
        if clip not in clippingareas: inputs_invalid = True

    if inputs_invalid:
        print("One or more inputs invalid - aborting")
        return admin()

    build_parameters = {
        'height_to_tip': height_to_tip,
        'blade_radius': blade_radius,
        'clip': clip,
        'custom_configuration': custom_configuration,
        'purge_all': purge_all
    }

    command_line = buildProcessingCommandLine(build_parameters)

    setProcessing(True, command_line)

    return redirect(url_for('serverlogs'))

@app.route("/processingstop")
def processingstop():
    """
    Stop processing
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    setProcessing(False)

    return redirect(url_for('settings'))

@app.route("/setdomain")
def setdomain():
    """
    Show set domain name page
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    return render_template("setdomain.html", error=None) 

@app.route("/processdomain", methods=['POST'])
def processdomain():
    """
    Process submitted domain name
    """

    if not isLoggedIn(): return redirect(url_for('login'))

    domain = request.form.get('domain', '').strip()

    try:
        domain_ip = socket.gethostbyname(domain).strip()
    except:
        domain_ip = None
    visible_ip = get('https://ipinfo.io/ip').text

    if domain_ip != visible_ip:
        return render_template("setdomain.html", error=domain) 

    with open('/usr/src/opensiteenergy/DOMAIN', 'w') as file: file.write("DOMAIN=" + domain)

    return redirect('http://' + visible_ip + '/redirectdomain?id=' + str(uuid.uuid4()) + '&domain=' + domain)

@app.route("/redirectdomain", methods=['GET'])
def redirectdomain():
    """
    Creates redirect page that shows result of Certbot
    If certbot log includes 'Successfully deployed', redirect to new SSL domain
    Uses non-secure IP address just to be safe when domain1 -> domain2
    """

    global CERTBOT_LOG

    if not isLoggedIn(): return redirect(url_for('login'))

    # Give opensiteenergy-servicesmanager.sh enough time to remove previous log-certbot.txt
    # Otherwise previously successful attempt will incorrectly appear as current success 
    # opensiteenergy-servicesmanager runs every 1s
    time.sleep(4)

    certbot_result, certbot_success = '', False
    if isfile(CERTBOT_LOG):
        with open(CERTBOT_LOG, "r", encoding='utf-8') as text_file: certbot_result = text_file.read().strip()

    if 'Successfully deployed certificate' in certbot_result: certbot_success = True

    visible_ip = get('https://ipinfo.io/ip').text
    domain = request.args.get('domain', '').strip()
    redirect_url = 'http://' + visible_ip + '/redirectdomain?id=' + str(uuid.uuid4())
    if (domain is not None) and (domain != ''): 
        if certbot_success: 
            redirect_url = 'https://' + domain + '/admin'
        else:
            redirect_url += '&domain=' + domain

    return render_template("redirectdomain.html", domain=domain, certbot_success=certbot_success, certbot_result=certbot_result, redirect_url=redirect_url)

# ***********************************************************
# ***********************************************************
# ********************* MAIN APPLICATION ********************
# ***********************************************************
# ***********************************************************

if 'SECRET_KEY' not in os.environ:
    addSecretKeyToEnv("../.env")
    load_dotenv("../.env")

app.secret_key = os.environ.get("SECRET_KEY")

if __name__ == '__main__':
    app.run(debug=True)
