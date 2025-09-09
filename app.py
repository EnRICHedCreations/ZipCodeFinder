#!/usr/bin/env python3
"""
Universal ZIP Code Geocoder - Web Application
Flask-based web interface for geocoding CSV files
"""

import os
import csv
import json
import time
import uuid
import threading
from datetime import datetime
from typing import Optional, Dict
from urllib.parse import quote
import requests
import re

from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['SECRET_KEY'] = 'geocoder-secret-key-2024'

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Global storage for processing sessions
processing_sessions = {}

class UniversalZipCodeGeocoder:
    def __init__(self, session_id):
        self.session_id = session_id
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Real Estate Lead Processor 1.0 (Contact: info@enrichedproperties.com)'
        })
        
        # Cache for performance
        self.geocode_cache = {}
        self.stats = {
            'total_processed': 0,
            'successful_geocodes': 0,
            'cache_hits': 0,
            'api_calls': 0,
            'failed_geocodes': 0,
            'cities_processed': set(),
            'zip_codes_found': set(),
            'current_address': '',
            'progress_percent': 0,
            'processing_complete': False,
            'results_log': []
        }
        self.stop_processing = False
    
    def clean_address(self, address: str) -> str:
        if not address:
            return ""
        
        address = address.strip().upper()
        address = re.sub(r'[^\w\s\-#/]', ' ', address)
        address = re.sub(r'\s+', ' ', address)
        address = re.sub(r'\s+UNIT\s+(\w+)', r' #\1', address)
        address = re.sub(r'\s+APT\s+(\w+)', r' #\1', address)
        
        return address.strip()
    
    def create_cache_key(self, address: str, city: str, state: str) -> str:
        clean_addr = self.clean_address(address)
        clean_city = city.strip().upper()
        clean_state = state.strip().upper()
        return f"{clean_addr}|{clean_city}|{clean_state}"
    
    def geocode_with_census(self, address: str, city: str, state: str) -> Optional[str]:
        try:
            full_address = f"{address}, {city}, {state}"
            
            url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
            params = {
                'address': full_address,
                'benchmark': 'Public_AR_Current',
                'format': 'json'
            }
            
            self.stats['api_calls'] += 1
            response = self.session.get(url, params=params, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                if 'result' in data and 'addressMatches' in data['result']:
                    matches = data['result']['addressMatches']
                    if matches and len(matches) > 0:
                        match = matches[0]
                        if 'addressComponents' in match:
                            zip_code = match['addressComponents'].get('zip')
                            if zip_code:
                                return zip_code[:5]
            
            time.sleep(0.25)
            return None
            
        except Exception:
            time.sleep(1)
            return None
    
    def geocode_with_nominatim(self, address: str, city: str, state: str) -> Optional[str]:
        try:
            full_address = f"{address}, {city}, {state}, United States"
            
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': full_address,
                'format': 'json',
                'addressdetails': '1',
                'limit': '1',
                'countrycodes': 'us'
            }
            
            self.stats['api_calls'] += 1
            response = self.session.get(url, params=params, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    address_details = data[0].get('address', {})
                    postcode = address_details.get('postcode')
                    if postcode:
                        zip_code = postcode.split('-')[0]
                        if len(zip_code) == 5 and zip_code.isdigit():
                            return zip_code
            
            time.sleep(1.5)
            return None
            
        except Exception:
            time.sleep(2)
            return None
    
    def get_zip_code(self, address: str, city: str, state: str) -> Optional[str]:
        if self.stop_processing:
            return None
            
        # Check cache first
        cache_key = self.create_cache_key(address, city, state)
        if cache_key in self.geocode_cache:
            self.stats['cache_hits'] += 1
            return self.geocode_cache[cache_key]
        
        # Clean inputs
        clean_address = self.clean_address(address)
        clean_city = city.strip().title()
        clean_state = state.strip().upper()
        
        # Track cities processed
        self.stats['cities_processed'].add(f"{clean_city}, {clean_state}")
        self.stats['current_address'] = f"{clean_address}, {clean_city}, {clean_state}"
        
        zip_code = None
        
        # Try Census Bureau first
        zip_code = self.geocode_with_census(clean_address, clean_city, clean_state)
        
        if not zip_code and not self.stop_processing:
            time.sleep(0.5)
            zip_code = self.geocode_with_nominatim(clean_address, clean_city, clean_state)
        
        # Cache the result
        self.geocode_cache[cache_key] = zip_code
        
        if zip_code:
            self.stats['successful_geocodes'] += 1
            self.stats['zip_codes_found'].add(zip_code)
            self.stats['results_log'].append({
                'type': 'success',
                'message': f"SUCCESS: {clean_address}, {clean_city}, {clean_state} → {zip_code}",
                'timestamp': datetime.now().strftime('%H:%M:%S')
            })
        else:
            self.stats['failed_geocodes'] += 1
            self.stats['results_log'].append({
                'type': 'error',
                'message': f"FAILED: {clean_address}, {clean_city}, {clean_state}",
                'timestamp': datetime.now().strftime('%H:%M:%S')
            })
        
        return zip_code

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.lower().endswith('.csv'):
        return jsonify({'error': 'Please upload a CSV file'}), 400
    
    # Create session ID and save file
    session_id = str(uuid.uuid4())
    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_{filename}")
    file.save(file_path)
    
    # Read CSV headers
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
    except Exception as e:
        os.remove(file_path)
        return jsonify({'error': f'Error reading CSV file: {str(e)}'}), 400
    
    return jsonify({
        'session_id': session_id,
        'filename': filename,
        'headers': headers
    })

@app.route('/process', methods=['POST'])
def process_file():
    data = request.json
    session_id = data.get('session_id')
    address_col = data.get('address_column')
    city_col = data.get('city_column')
    state_col = data.get('state_column')
    zip_col = data.get('zip_column')
    
    if not all([session_id, address_col, city_col, state_col, zip_col]):
        return jsonify({'error': 'Missing required parameters'}), 400
    
    # Find the uploaded file
    upload_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.startswith(session_id)]
    if not upload_files:
        return jsonify({'error': 'Session file not found'}), 404
    
    input_file = os.path.join(app.config['UPLOAD_FOLDER'], upload_files[0])
    output_file = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_geocoded.csv")
    
    # Create geocoder instance
    geocoder = UniversalZipCodeGeocoder(session_id)
    processing_sessions[session_id] = geocoder
    
    # Start processing in background thread
    thread = threading.Thread(
        target=process_csv_file,
        args=(geocoder, input_file, output_file, address_col, city_col, state_col, zip_col)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({'message': 'Processing started'})

def process_csv_file(geocoder, input_file, output_file, address_col, city_col, state_col, zip_col):
    try:
        # Count total rows
        with open(input_file, 'r', encoding='utf-8') as f:
            total_rows = sum(1 for _ in csv.DictReader(f))
        
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                reader = csv.DictReader(infile)
                writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                writer.writeheader()
                
                for row in reader:
                    if geocoder.stop_processing:
                        break
                        
                    geocoder.stats['total_processed'] += 1
                    
                    # Update progress
                    progress = int((geocoder.stats['total_processed'] / total_rows) * 100)
                    geocoder.stats['progress_percent'] = progress
                    
                    current_zip = row.get(zip_col, '').strip()
                    address = row.get(address_col, '').strip()
                    city = row.get(city_col, '').strip()
                    state = row.get(state_col, '').strip()
                    
                    if not address or not city or not state:
                        writer.writerow(row)
                        continue
                    
                    # Process if zip is missing or invalid
                    if not current_zip or len(current_zip) != 5 or not current_zip.isdigit():
                        zip_code = geocoder.get_zip_code(address, city, state)
                        if zip_code:
                            row[zip_col] = zip_code
                    
                    writer.writerow(row)
        
        geocoder.stats['processing_complete'] = True
        
    except Exception as e:
        geocoder.stats['results_log'].append({
            'type': 'error',
            'message': f"Processing error: {str(e)}",
            'timestamp': datetime.now().strftime('%H:%M:%S')
        })

@app.route('/status/<session_id>')
def get_status(session_id):
    geocoder = processing_sessions.get(session_id)
    if not geocoder:
        return jsonify({'error': 'Session not found'}), 404
    
    stats = geocoder.stats.copy()
    # Convert sets to lists for JSON serialization
    stats['cities_processed'] = list(stats['cities_processed'])
    stats['zip_codes_found'] = list(stats['zip_codes_found'])
    
    return jsonify(stats)

@app.route('/stop/<session_id>', methods=['POST'])
def stop_processing(session_id):
    geocoder = processing_sessions.get(session_id)
    if not geocoder:
        return jsonify({'error': 'Session not found'}), 404
    
    geocoder.stop_processing = True
    return jsonify({'message': 'Processing stopped'})

@app.route('/download/<session_id>')
def download_file(session_id):
    output_file = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_geocoded.csv")
    if not os.path.exists(output_file):
        return jsonify({'error': 'Processed file not found'}), 404
    
    return send_file(output_file, as_attachment=True, download_name='geocoded_results.csv')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)