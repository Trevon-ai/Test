import os
import msal
import requests
import pandas as pd
import io
import time
import datetime
import json
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, render_template_string, jsonify
import threading
import logging
import random
from requests.exceptions import RequestException

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dashboard.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("dashboard")

# Flask application setup
app = Flask(__name__)

# Add a lock for thread safety
token_lock = threading.Lock()

DEFAULT_SHEET_NAME = "ProductionSchedule"
MACHINE_TYPES = ["KONICA", "XEROX"]

data_storage = {
    "df_hp": pd.DataFrame(),
    "df_xerox": pd.DataFrame(),
    "machine": "HP",
}

# Microsoft Graph API configuration
CLIENT_ID = os.environ.get("MS_CLIENT_ID", "152e9302-0abc-4618-b877-e53018a443b7")
TENANT_ID = "consumers"  # Use "consumers" for personal Microsoft accounts
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["Files.Read", "Files.Read.All"]
GRAPH_FILE_PATH = os.environ.get("GRAPH_FILE_PATH", "/Documents/PDS_ProdSchedule.xlsx")

# File path to store token cache
TOKEN_CACHE_FILE = "token_cache.json"

# Store the access token details globally
current_token_info = {
    "access_token": None,
    "expires_at": None,
    "is_new_token": False
}

def get_access_token(max_retries=3, base_backoff=2):
    """
    Get access token using device code flow with network error handling
    """
    global current_token_info
    
    retries = 0
    while retries <= max_retries:
        try:
            with token_lock:  # Lock the entire function for thread safety
                # Reset the is_new_token flag at the beginning
                current_token_info["is_new_token"] = False
                previous_token = current_token_info.get("access_token")
                
                # Create token cache
                cache = msal.SerializableTokenCache()
                
                # Load the token cache from file if it exists
                if os.path.exists(TOKEN_CACHE_FILE):
                    try:
                        with open(TOKEN_CACHE_FILE, 'r') as token_file:
                            cache_data = token_file.read()
                            cache.deserialize(cache_data)
                            logger.debug(f"Loaded token cache: {len(cache_data)} bytes")
                    except Exception as e:
                        logger.error(f"Failed to load token cache: {str(e)}")
                
                # Create MSAL app
                app = msal.PublicClientApplication(
                    client_id=CLIENT_ID,
                    authority=AUTHORITY,
                    token_cache=cache
                )
                
                # Try to get token silently from cache first
                accounts = app.get_accounts()
                
                if accounts:
                    logger.debug("Checking for cached token...")
                    result = app.acquire_token_silent(SCOPES, account=accounts[0])
                    if result and "access_token" in result:
                        # Update token info
                        token_value = result["access_token"]
                        
                        # Check if this is a new token
                        if token_value != previous_token and previous_token is not None:
                            current_token_info["is_new_token"] = True
                            logger.info("🔄 ACCESS TOKEN WAS REFRESHED IN THIS ITERATION 🔄")
                            
                            # Save the updated token cache if it has changed
                            if cache.has_state_changed:
                                _save_token_cache(cache)
                        else:
                            logger.info("Using existing access token")
                        
                        # Update token info with all relevant data
                        _update_token_info(result)
                        _log_token_expiration()
                        
                        return result
                
                # If no token in cache or expired, use device code flow
                logger.info("No valid token in cache. Starting device code flow...")
                flow = app.initiate_device_flow(scopes=SCOPES)
                
                if "user_code" not in flow:
                    error_msg = f"Failed to create device flow: {flow.get('error')}. Error description: {flow.get('error_description')}"
                    logger.error(error_msg)
                    print(error_msg)
                    # This could be a network error, so retry
                    raise RequestException("Failed to create device flow")
                
                print("\n" + "=" * 50)
                print(flow["message"])  # Shows the message with URL and code for user to authenticate
                print("=" * 50 + "\n")
                
                # Complete the flow by polling for token
                result = app.acquire_token_by_device_flow(flow)
                
                # Save token cache
                if "access_token" in result and cache.has_state_changed:
                    _save_token_cache(cache)
                
                # Update token info
                if "access_token" in result:
                    current_token_info["is_new_token"] = True
                    logger.info("New access token obtained via interactive login!")
                    _update_token_info(result)
                    _log_token_expiration()
                
                return result
                
        except (requests.exceptions.RequestException, ConnectionError, TimeoutError) as e:
            if retries < max_retries:
                backoff_time = base_backoff * (2 ** retries) + random.uniform(0, 1)
                logger.error(f"Network error during token acquisition: {e}. Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
                retries += 1
            else:
                logger.error(f"Failed to acquire token after {max_retries} attempts due to network errors: {e}")
                return {"error": "network_error", "error_description": str(e)}
    
    # Should never reach here, but just in case
    return {"error": "max_retries_exceeded", "error_description": "Failed after maximum retries"}

def _update_token_info(result):
    """Helper function to update the token info with data from result"""
    global current_token_info
    
    current_token_info["access_token"] = result.get("access_token")
    
    # Calculate and store expiration time
    expires_in = result.get("expires_in", 3600)  # Default to 1 hour if not specified
    current_token_info["expires_at"] = datetime.now() + timedelta(seconds=expires_in)


def _log_token_expiration():
    """Helper function to log token expiration information"""
    time_until_expiry = current_token_info["expires_at"] - datetime.now()
    logger.info(f"Access token expires at: {current_token_info['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Time until expiration: {time_until_expiry}")


def _save_token_cache(cache):
    """Helper function to save token cache to file"""
    try:
        with open(TOKEN_CACHE_FILE, 'w') as token_file:
            token_file.write(cache.serialize())
            logger.info("Token cache saved to file.")
    except Exception as e:
        logger.error(f"Failed to save token cache: {str(e)}")


def get_excel_data(max_retries=5, base_backoff=1, timeout=30):
    """
    Access Excel file through Microsoft Graph API and load specific sheet
    
    Args:
        max_retries: Maximum number of retry attempts for network failures
        base_backoff: Base delay time in seconds for exponential backoff
        timeout: Request timeout in seconds
        
    Returns:
        Pandas DataFrame if successful, None otherwise
    """
    # Get access token - call only once at the beginning
    token_result = get_access_token()
    
    if "access_token" not in token_result:
        print(f"Error getting access token: {token_result.get('error')}")
        print(f"Error description: {token_result.get('error_description')}")
        return None
    
    access_token = token_result["access_token"]
    
    # Build API request
    api_endpoint = "https://graph.microsoft.com/v1.0/me"
    file_endpoint = f"{api_endpoint}/drive/root:{GRAPH_FILE_PATH}:/content"
    
    # Set up headers with access token
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
    }
    
    # Get file content with backoff and error handling
    file_content = get_file_with_backoff(file_endpoint, headers, max_retries, base_backoff, timeout)
    
    if file_content is None:
        print("Failed to retrieve Excel file")
        return None
    
    # Load Excel data from response content
    print("File accessed successfully, loading Excel data...")
    try:
        excel_data = pd.ExcelFile(io.BytesIO(file_content))
        
        # Check if the specified sheet exists
        sheet_name = DEFAULT_SHEET_NAME
        if sheet_name not in excel_data.sheet_names:
            print(f"Sheet '{sheet_name}' not found. Available sheets: {excel_data.sheet_names}")
            return None
        
        # Read the specific sheet
        df = pd.read_excel(excel_data, sheet_name)
        print(f"Successfully loaded sheet '{sheet_name}'")
        
        # Filter out rows where 'Print Priority' is NaN - more efficient syntax
        mask = df['Print Priority'].notna()
        df = df.loc[mask]
        
        # Convert PrdOrd to integer
        df['PrdOrd'] = pd.to_numeric(df['PrdOrd'], errors='coerce').astype('Int64')

        # Convert PrdOrd to integer
        df['Print Priority'] = pd.to_numeric(df['Print Priority'], errors='coerce').astype('Int64')

        # Format Qty with thousand separators
        df['Qty'] = df['Qty'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "")

        # Convert to datetime safely — invalid or empty values become NaT (Not a Time)
        df['Delivery Expected'] = pd.to_datetime(df['Delivery Expected'], errors='coerce')

        # Format date to '10-March', and keep empty cells as empty strings
        df['Delivery Expected'] = df['Delivery Expected'].apply(
            lambda x: x.strftime('%d-%B').lstrip('0') if pd.notna(x) else ''
        )

        # Convert machine names to uppercase once for efficiency
        df['Machine_upper'] = df['Machine'].str.upper()
        
        # Sort by 'Print Priority', keep None/NaN at the bottom
        df = df.sort_values(by='Print Priority', na_position='last')

        # Create filtered dataframes for each machine type
        machine_dataframes = {}
        for machine in MACHINE_TYPES:
            filtered_df = df.loc[df['Machine_upper'] == machine, ['PrdOrd','SO No', 'Job','Qty', 'Time (hrs)','Print Priority','Delivery Expected']]
            machine_dataframes[f"df_{machine.lower()}"] = filtered_df
            print(f"Filtered {machine} data: {filtered_df.shape[0]} rows")
            
            # Store the filtered data in the data storage
            data_storage[f"df_{machine.lower()}"] = filtered_df
        
        # Remove temporary column
        df.drop('Machine_upper', axis=1, inplace=True)

        return df
    
    except Exception as e:
        print(f"Error parsing Excel file: {str(e)}")
        return None

def get_file_with_backoff(file_endpoint, headers, max_retries=5, base_backoff=1, timeout=10):
    """
    Get file content with exponential backoff for retries.
    
    Args:
        file_endpoint: URL of the file to download
        headers: Request headers with token already included
        max_retries: Maximum number of retry attempts
        base_backoff: Base delay time in seconds for exponential backoff
        timeout: Request timeout in seconds
        
    Returns:
        Response content if successful, None otherwise
    """
    print(f"Requesting file: {file_endpoint}")
    
    retries = 0
    
    while retries <= max_retries:
        try:
            response = requests.get(file_endpoint, headers=headers, timeout=timeout)
            
            # Handle different HTTP status codes
            if response.status_code == 200:
                return response.content
                
            elif response.status_code in (401, 403):  
                # Don't retry auth errors - token was already provided by caller
                print(f"Authentication error: {response.status_code}. Token might be invalid.")
                return None
                    
            elif response.status_code == 429:  # Too Many Requests
                # Check for Retry-After header
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    sleep_time = int(retry_after)
                    print(f"Rate limited. Waiting for {sleep_time} seconds as specified by server.")
                    time.sleep(sleep_time)
                    continue  # Retry immediately after waiting
                
            elif response.status_code >= 500:  # Server errors
                print(f"Server error: {response.status_code}. Retrying...")
                
            elif response.status_code >= 400:  # Other client errors
                print(f"Client error: {response.status_code}. {response.text}")
                return None  # Don't retry other client errors
            
            # If we get here, we need to retry with backoff
            if retries < max_retries:
                backoff_time = base_backoff * (2 ** retries) + random.uniform(0, 1)
                print(f"Attempt {retries + 1} failed. Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
                retries += 1
            else:
                print(f"Failed after {max_retries} attempts. Status code: {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            if retries < max_retries:
                backoff_time = base_backoff * (2 ** retries) + random.uniform(0, 1)
                print(f"Request timed out. Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
                retries += 1
            else:
                print(f"Request timed out after {max_retries} attempts.")
                return None
                
        except RequestException as e:
            if retries < max_retries:
                backoff_time = base_backoff * (2 ** retries) + random.uniform(0, 1)
                print(f"Network error: {e}. Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)
                retries += 1
            else:
                print(f"Network error after {max_retries} attempts: {e}")
                return None
    
    return None


def main():
    """
    Main function to test Microsoft Graph API token refresh monitoring
    """
    print("Starting Microsoft Graph API token refresh monitor...")
    print("Will attempt to access Excel data every minute")
    print("Press Ctrl+C to exit")
    

    while True:
        print("\n" + "=" * 50)
        print(f"Data update attempt at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check if we have token info and display time until expiry
        if current_token_info["expires_at"]:
            time_until_expiry = current_token_info["expires_at"] - datetime.now()
            hours, remainder = divmod(time_until_expiry.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            print(f"Current token expires in: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        
        # Try to get Excel data
        df = get_excel_data()
        
        """
        if df is not None:
            print("\nExcel Data Preview (First 3 rows):")
            print(df.head(3))
            print(f"Total rows: {len(df)}")
        else:
            print("Failed to retrieve Excel data.")
        """
    
        print("=" * 50)
        
        # Wait for 60 seconds before the next update
        print(f"Waiting 60 seconds until next update...")
        time.sleep(60)
            
def switch_machine():
     while True:
        current_machine = data_storage['machine']
        print(f"Switching machine from {current_machine}")
        
        # Find the next machine from the MACHINE_TYPES list
        current_index = MACHINE_TYPES.index(current_machine) if current_machine in MACHINE_TYPES else -1
        next_index = (current_index + 1) % len(MACHINE_TYPES)
        data_storage["machine"] = MACHINE_TYPES[next_index]
        
        print(f"Switched to {data_storage['machine']}")
        time.sleep(30)  # Total of 30 seconds per machine



# HTML template (dark theme with rounded dashboard)
dashboard_html = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script>
        // Reload page every 30 seconds
        setTimeout(() => {
            window.location.reload();
        }, 30500);
    </script>

    <title>Machine Dashboard</title>
   
    <style>
        html, body {
            height: 100%;
            margin: 0;
            padding: 0;
            overflow: hidden; /* Prevent body scrollbar */
        }
        body {
            background-color: #121b24;
            color: #cce6ff;
            font-family: 'Courier New', Courier, monospace;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        .dashboard {
            border: 2px solid #4fa3d1;
            border-radius: 20px;
            padding: 25px;
            width: 90%;
            height: 90vh;
            max-width: 1400px;
            box-shadow: 0 0 20px #4fa3d1;
            display: flex;
            flex-direction: column; 
            animation: fadeIn 0.8s ease-in;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: scale(0.98); }
            to { opacity: 1; transform: scale(1); }
        }
        .header-section {
            flex: 0 0 auto;
            padding-bottom: 15px;
            margin-bottom: 15px;
            border-bottom: 2px solid #4fa3d1;
        }
        .table-section {
            flex: 1 1 auto;
            position: relative;
            overflow: hidden;
        }
        .table-container {
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            overflow-y: auto; /* Enable vertical scrolling */
            /* Remove the scrollbar hiding styles */
        }
                .table-container::-webkit-scrollbar {
            width: 8px;
        }
        .table-container::-webkit-scrollbar-track {
            background: #1d2b38;
            border-radius: 4px;
        }
        .table-container::-webkit-scrollbar-thumb {
            background-color: #4fa3d1;
            border-radius: 4px;
        }
        .footer-section {
            flex: 0 0 auto;
            padding-top: 15px;
            margin-top: 15px;
            border-top: 1px solid #4fa3d1;
        }
        h1, h2 {
            text-align: center;
            margin-top: 0;
            margin-bottom: 10px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        thead {
            position: sticky;
            top: 0;
            background-color: #121b24;
            z-index: 2;
        }
        th, td {
            border-bottom: 1px solid #4fa3d1;
            padding: 10px 15px;
            text-align: left;
        }
        th {
            font-size: 1.2em;
            background-color: #172535;
        }
        td {
            font-size: 1.1em;
            white-space: nowrap; /* Prevents wrapping */
            text-overflow: ellipsis; /* Adds '...' when the text overflows */
        }
        .footer {
            font-size: 0.9em;
            text-align: center;
            color: #7aaed6;
        }
        .status {
            text-align: center;
            margin-top: 10px;
            font-size: 0.85em;
            color: #7aaed6;
        }
        .no-data {
            text-align: center;
            margin: 30px 0;
            font-style: italic;
            color: #7aaed6;
        }
        .auth-alert {
            background-color: #d14f4f;
            color: white;
            padding: 15px;
            margin: 10px 0;
            border-radius: 10px;
            text-align: center;
            font-weight: bold;
        }
        .fade-row {
            opacity: 0;
            animation: fadeInRow 0.4s forwards;
        }
        @keyframes fadeInRow {
            to {
                opacity: 1;
            }
        }
    </style>
</head>
<body>
    <div class="dashboard">

        <div class="header-section">
            {% if auth_required %}
            <div class="auth-alert">
                Authentication Required - Please check the console for login instructions
            </div>
            {% endif %}
            
            <h1>{{ machine }} Production Schedule</h1>
        </div>
        
        <div class="table-section">
            <div class="table-container">
                {% if rows|length > 0 %}
                <table>
                    <thead>
                        <tr>
                            <th>PrdOrd</th>
                            <th>SO No</th>
                            <th>Job</th>
                            <th>Qty</th>
                            <th>Time (hrs)</th>
                            <th>Print Priority</th>
                            <th>Delivery Expected</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in rows %}
                        <tr class="fade-row" style="animation-delay: {{ loop.index0 * 0.05 }}s;">
                            <td>{{ row['PrdOrd'] }}</td>
                            <td>{{ row['SO No'] }}</td>
                            <td>{{ row['Job'] }}</td>
                            <td>{{ row['Qty'] }}</td>
                            <td>{{ row['Time (hrs)'] }}</td>
                            <td>{{ row['Print Priority'] }}</td>
                            <td>{{ row['Delivery Expected'] }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
                {% else %}
                <div class="no-data">No production data available for {{ machine }}</div>
                {% endif %}
            </div>
        </div>
        
        <div class="footer-section">
            <div class="footer">Refreshing data every 60 seconds | View switching every 15 seconds</div>
        </div>
    </div>
</body>
</html>
'''


@app.route("/")
def dashboard():
    machine = data_storage["machine"]
    # Use the lowercase machine name to access the corresponding dataframe
    df = data_storage.get(f"df_{machine.lower()}")
    
    # Fallback if the dataframe doesn't exist
    if df is None:
        df = pd.DataFrame(columns=['SO No', 'Job', 'PrdOrd', 'Time (hrs)', 'Qty', 'Print Priority', 'Delivery Expected'])
        
    return render_template_string(dashboard_html, 
                                  rows=df.to_dict(orient="records"), 
                                  machine=machine)

if __name__ == "__main__":
    # Initialize machine to first machine in the list if not already set
    if "machine" not in data_storage:
        data_storage["machine"] = MACHINE_TYPES[0] if MACHINE_TYPES else "XEROX"

    threading.Thread(target=main, daemon=True).start()
    threading.Thread(target=switch_machine, daemon=True).start()
    app.run(host="0.0.0.0", port=8000, debug=False)