import flet as ft
import threading
import os
import sys
import time
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import traceback

# === Config ===
USER_SPECIFIED_DEFAULT_DOWNLOAD_PATH = "G:\\UsersMy Drive\\0investment\\0ravi\\Promoter Data for sheet"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
GOOGLE_SHEET_ID = 'YOUR_GOOGLE_SHEET_ID_HERE'  # <--- !!! CRITICAL: REPLACE THIS !!!
WORKSHEET_NAME = 'bse_insider_data'
MAX_STATUS_LABEL_LINES = 30

# === Thread-Safe UI Update Helpers ===
def _update_ui(page, task_logic_func, *args_for_task_logic):
    def scheduled_task_wrapper():
        if page and page.session_id:
            try:
                task_logic_func(*args_for_task_logic)
            except Exception as e:
                original_stderr = getattr(sys, '__stderr__', sys.stderr)
                print(f"Error during scheduled UI task execution: {e}", file=original_stderr)
                traceback.print_exc(file=original_stderr)
        else:
            original_stdout = getattr(sys, '__stdout__', sys.stdout)
            print("Debug (from _update_ui): UI update skipped, page closed before task execution.", file=original_stdout)

    if page and page.session_id:
        page.call_soon_threadsafe(scheduled_task_wrapper)
    else:
        original_stdout = getattr(sys, '__stdout__', sys.stdout)
        print("Debug (from _update_ui): UI update skipped, page closed before queueing.", file=original_stdout)

def set_control_value(page, control, new_value):
    def core_task():
        control.value = new_value
        if hasattr(control, 'update'): control.update()
    _update_ui(page, core_task)

def set_control_disabled(page, control, is_disabled):
    def core_task():
        control.disabled = is_disabled
        if hasattr(control, 'update'): control.update()
    _update_ui(page, core_task)

def append_text_to_control(page, control, new_log_entry, max_visible_lines=MAX_STATUS_LABEL_LINES):
    def core_task():
        new_lines_to_add = [line for line in new_log_entry.split('\n') if line.strip()]
        current_text = control.value if control.value else ""
        current_lines = [line for line in current_text.split('\n') if line.strip()]
        current_lines.extend(new_lines_to_add)
        if len(current_lines) > max_visible_lines:
            current_lines = current_lines[-max_visible_lines:]
        control.value = "\n".join(current_lines)
        if hasattr(control, 'update'): control.update()
    _update_ui(page, core_task)

# === Class for Redirecting print() Output to Flet UI ===
class RedirectOutput:
    def __init__(self, page_ref, status_label_ref, max_lines_in_label=MAX_STATUS_LABEL_LINES):
        self.page = page_ref
        self.status_label = status_label_ref
        self.max_lines = max_lines_in_label
        self.buffer = ""
        self.original_stdout = None
        self.original_stderr = None

    def write(self, text):
        if not (self.page and self.page.session_id):
            if self.original_stdout:
                self.original_stdout.write(f"(UI Page Gone) {text}")
            return
        self.buffer += text
        if '\n' in self.buffer:
            lines_to_send, self.buffer = self.buffer.rsplit('\n', 1)
            if lines_to_send:
                append_text_to_control(self.page, self.status_label, lines_to_send, self.max_lines)
            if self.buffer == '\n':
                self.buffer = ""

    def flush(self):
        if self.page and self.page.session_id and self.buffer:
            append_text_to_control(self.page, self.status_label, self.buffer, self.max_lines)
            self.buffer = ""
        elif self.buffer and self.original_stdout:
             self.original_stdout.write(f"(UI Page Gone - Flush) {self.buffer}")
             self.buffer = ""

    def __enter__(self):
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
        sys.stdout = self
        sys.stderr = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr
        if exc_type:
            error_message = f"ERROR in task: {exc_type.__name__}: {str(exc_val).splitlines()[0]}"
            if self.page and self.page.session_id:
                append_text_to_control(self.page, self.status_label, error_message, self.max_lines)
            if self.original_stderr:
                print(f"\n--- Worker Thread Exception (UI attempted for summary) ---", file=self.original_stderr)
                traceback.print_exception(exc_type, exc_val, exc_tb, file=self.original_stderr)
                print(f"--- End Worker Thread Exception ---", file=self.original_stderr)

# === Google Sheets Functions ===
def get_gspread_client():
    creds = None
    if os.path.exists(TOKEN_FILE):
        print("GS: Loading token...")
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("GS: Refreshing token...")
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"GS: Token refresh error: {e}")
                return None
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"GS ERROR: '{CREDENTIALS_FILE}' not found! Please place it in the script directory.")
                return None
            print("GS: Starting OAuth (check console/browser for prompt)...")
            try:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                print("GS: Please follow the OAuth prompts that may appear in your browser or console.")
                creds = flow.run_local_server(port=0)
            except Exception as e:
                print(f"GS: OAuth error: {e}")
                return None
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
            print("GS: Token saved.")
    print("GS: Authorizing client...")
    try:
        client = gspread.authorize(creds)
        print("GS: Client authorized.")
        return client
    except Exception as e:
        print(f"GS: Client auth error: {e}")
        return None

def upload_df_to_sheet(df, sheet_id_param, worksheet_name_param):
    try:
        print("GS: Connecting for upload...")
        gc = get_gspread_client()
        if not gc:
            return False
        spreadsheet = gc.open_by_key(sheet_id_param)
        worksheet = None
        try:
            worksheet = spreadsheet.worksheet(worksheet_name_param)
        except gspread.exceptions.WorksheetNotFound:
            print(f"GS: Worksheet '{worksheet_name_param}' not found. Creating...")
            try:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name_param, rows=max(100, df.shape[0]+10), cols=max(20, df.shape[1] + 5))
            except Exception as e_create:
                print(f"❌ GS: Create worksheet error: {e_create}")
                return False
        print(f"GS: Clearing '{worksheet_name_param}'...")
        worksheet.clear()
        df_upload = df.fillna('')
        print(f"GS: Uploading {df_upload.shape[0]} rows to '{worksheet_name_param}'...")
        data_to_upload = [df_upload.columns.values.tolist()] + df_upload.values.tolist()
        worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
        ws_id_url_part = f"#gid={worksheet.id}" if hasattr(worksheet, 'id') else ""
        print(f"✅ GS: Uploaded! URL: {spreadsheet.url}{ws_id_url_part}")
        return True
    except Exception as e:
        error_msg = f"❌ GS Upload Error: {type(e).__name__}: {str(e).splitlines()[0]}"
        if isinstance(e, gspread.exceptions.APIError) and "PERMISSION_DENIED" in str(e):
            error_msg += " Check sheet sharing."
        print(error_msg)
        original_stderr = getattr(sys, '__stderr__', sys.stderr)
        if original_stderr:
             print(f"Detailed Google Sheets Upload Error (Console):\n{traceback.format_exc()}", file=original_stderr)
        return False

# === Main Selenium and Processing Logic ===
def run_downloader_and_uploader_task(target_url, resolved_download_dir, page_ref, status_label_ref, send_button_ref):
    set_control_disabled(page_ref, send_button_ref, True)

    with RedirectOutput(page_ref, status_label_ref, MAX_STATUS_LABEL_LINES):
        print(f"Process starting for URL: {target_url}")
        print(f"Using download directory: {resolved_download_dir}")
        try:
            os.makedirs(resolved_download_dir, exist_ok=True)
            print(f"Ensured download directory exists or was created: {resolved_download_dir}")
        except OSError as e:
            print(f"❌ Error creating download directory '{resolved_download_dir}': {e}")
            set_control_disabled(page_ref, send_button_ref, False)
            return

        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": resolved_download_dir,
            "download.prompt_for_download": False,
            "directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument('--log-level=3')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")

        driver = None
        downloaded_file_path = None # Initialize to ensure it's defined
        try:
            print("Driver: Setting up Chrome WebDriver...")
            try:
                os.environ['WDM_LOG_LEVEL'] = '0'
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                print("Driver: WebDriver is ready (running headlessly).")
            except Exception as e_driver:
                print(f"❌ Driver setup error: {e_driver}")
                return

            wait = WebDriverWait(driver, 40)
            print(f"Navigating to target URL...")
            driver.get(target_url)
            print("Navigation complete.")

            download_button_id = "downloadlnk"
            print(f"Waiting for download button (ID: {download_button_id})...")
            download_btn_element = wait.until(EC.element_to_be_clickable((By.ID, download_button_id)))

            print("Button found. Attempting to click download link...")
            driver.execute_script("arguments[0].click();", download_btn_element)
            print("Download action triggered. Monitoring download directory...")

            timeout_seconds = 90
            start_time = time.time()
            # downloaded_file_path = None # Already initialized above
            last_monitor_update_time = 0

            while time.time() - start_time < timeout_seconds:
                current_files = os.listdir(resolved_download_dir)
                data_files = [f for f in current_files if (f.lower().endswith((".xlsx", ".xls", ".csv"))) and not f.lower().startswith("~$") and not f.lower().endswith((".tmp", ".crdownload"))]

                if data_files:
                    data_files.sort(key=lambda f_name: os.path.getmtime(os.path.join(resolved_download_dir, f_name)), reverse=True)
                    potential_file = os.path.join(resolved_download_dir, data_files[0])
                    time.sleep(3) # Give a bit of time for the file to be fully written
                    if os.path.exists(potential_file) and os.path.getsize(potential_file) > 0:
                        downloaded_file_path = potential_file
                        break

                current_time = time.time()
                if current_time - last_monitor_update_time > 5 or last_monitor_update_time == 0 :
                    elapsed_time = int(current_time - start_time)
                    print(f"Monitoring download... ({elapsed_time}s / {timeout_seconds}s)")
                    last_monitor_update_time = current_time
                time.sleep(0.5)

            if downloaded_file_path:
                base_name = os.path.basename(downloaded_file_path)
                print(f"---")
                print(f"✅ File download detected: {base_name}")
                print(f"   File saved to: {downloaded_file_path}") 
                sys.stdout.flush() # <--- ADDED THIS LINE
                print(f"Reading '{base_name}' with Pandas...")

                df = None
                try:
                    if downloaded_file_path.lower().endswith((".xlsx", ".xls")):
                        df = pd.read_excel(downloaded_file_path)
                    elif downloaded_file_path.lower().endswith(".csv"):
                        try:
                            df = pd.read_csv(downloaded_file_path, encoding='utf-8')
                        except UnicodeDecodeError:
                            print("   CSV UTF-8 decoding failed, trying 'latin1'...")
                            df = pd.read_csv(downloaded_file_path, encoding='latin1')

                    if df is not None:
                        print(f"Successfully read data from '{base_name}'. Shape: {df.shape}.")
                        print(f"Preparing for Google Sheets upload...")

                        if GOOGLE_SHEET_ID and GOOGLE_SHEET_ID != 'YOUR_GOOGLE_SHEET_ID_HERE':
                            upload_successful = upload_df_to_sheet(df, GOOGLE_SHEET_ID, WORKSHEET_NAME)
                            if upload_successful:
                               print(f"Google Sheets upload done. Deleting local file: {base_name}")
                               try:
                                   os.remove(downloaded_file_path)
                                   print(f"   Local file '{base_name}' was deleted.")
                               except Exception as e_del:
                                   print(f"   Error deleting local file '{base_name}': {e_del}")
                            else:
                               print(f"Google Sheets upload FAILED. Local file retained: {downloaded_file_path}")
                        else:
                            print(f"⚠️ Google Sheet ID not configured. Upload SKIPPED.")
                            print(f"   File retained at: {downloaded_file_path}")

                        print(f"---")
                        print(f"✅ Task COMPLETED for: {base_name}")
                        print(f"   Downloaded to: {downloaded_file_path if os.path.exists(downloaded_file_path) else 'File was deleted after processing'}")
                    else:
                        print(f"❌ Unsupported file type for processing: {base_name}")
                except Exception as e_proc:
                    print(f"❌ Error processing downloaded file '{base_name}': {str(e_proc).splitlines()[0]}")
            else:
                print(f"---")
                print(f"❌ File download timed out after {timeout_seconds} seconds.")
                print(f"   Contents of download directory '{resolved_download_dir}': {os.listdir(resolved_download_dir) if os.path.exists(resolved_download_dir) else 'Directory not found or inaccessible'}")

        except Exception as e_task:
            print(f"❌ UNEXPECTED ERROR in main task: {type(e_task).__name__}: {str(e_task).splitlines()[0]}")
        finally:
            if driver:
                driver.quit()
                print("Browser (headless) has been closed.")
            print("--- Task execution finished ---")

    set_control_disabled(page_ref, send_button_ref, False)

# === Flet Application Main Function ===
def main(page: ft.Page):
    page.title = "BSE Data Processor"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.vertical_alignment = ft.MainAxisAlignment.START
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
    page.window_width = 750
    page.window_height = 700
    page.padding = 20

    url_textfield = ft.TextField(
        label="Enter BSE India Disclosures URL",
        hint_text="e.g., https://www.bseindia.com/.../disclosures-insider-trading-2015/",
        autofocus=True,
    )

    custom_location_textfield = ft.TextField(
        label="Enter custom download location",
        hint_text="e.g., D:\\My BSE Downloads",
        visible=False,
        expand=True
    )

    default_location_checkbox = ft.Checkbox(
        label="Download at default location",
        value=True,
    )

    def toggle_custom_location_field(e):
        is_checked = default_location_checkbox.value
        custom_location_textfield.visible = not is_checked
        if is_checked:
            custom_location_textfield.value = ""
            custom_location_textfield.error_text = None
        page.update()

    default_location_checkbox.on_change = toggle_custom_location_field

    status_label = ft.Text(
        "Status updates and logs will appear here.",
        max_lines=MAX_STATUS_LABEL_LINES,
        overflow=ft.TextOverflow.CLIP,
        selectable=True,
        font_family="monospace"
    )

    send_button = ft.ElevatedButton(
        text="Download & Upload",
        icon="play_arrow_rounded",
        width=250,
        height=50
    )

    def send_button_clicked(e):
        entered_url = url_textfield.value.strip()
        url_valid = True
        download_path_valid = True
        actual_download_dir = ""

        if not entered_url:
            url_textfield.error_text = "URL cannot be empty."
            url_valid = False
        elif not (entered_url.startswith("http://") or entered_url.startswith("https://")):
            url_textfield.error_text = "Invalid URL format (must start with http:// or https://)."
            url_valid = False
        else:
            url_textfield.error_text = None
        url_textfield.update()

        if default_location_checkbox.value:
            actual_download_dir = USER_SPECIFIED_DEFAULT_DOWNLOAD_PATH
            if custom_location_textfield.error_text:
                 custom_location_textfield.error_text = None
                 custom_location_textfield.update()
        else:
            actual_download_dir = custom_location_textfield.value.strip()
            if not actual_download_dir:
                custom_location_textfield.error_text = "Custom location cannot be empty if default is unchecked."
                download_path_valid = False
            else:
                custom_location_textfield.error_text = None
            custom_location_textfield.update()

        if not url_valid or not download_path_valid:
            error_messages = []
            if not url_valid and url_textfield.error_text:
                error_messages.append(url_textfield.error_text)
            if not download_path_valid and custom_location_textfield.error_text:
                error_messages.append(custom_location_textfield.error_text)
            
            status_error_message = "Error: " + " | ".join(msg for msg in error_messages if msg)
            if not status_error_message.endswith("Error: "):
                 set_control_value(page, status_label, status_error_message)
            else:
                 set_control_value(page, status_label, "Error: Please correct the highlighted fields.")
            return

        set_control_value(page, status_label, f"Initiating process for: {entered_url}\nDownload path: {actual_download_dir}\n---")

        thread = threading.Thread(
            target=run_downloader_and_uploader_task,
            args=(entered_url, actual_download_dir, page, status_label, send_button)
        )
        thread.daemon = True
        thread.start()

    send_button.on_click = send_button_clicked
    url_textfield.on_submit = send_button_clicked

    page.add(
        ft.Column(
            controls=[
                ft.Text("BSE Insider Trading Data Processor", size=24, weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER),
                ft.Text("Downloads data from BSE India and uploads to Google Sheets.", text_align=ft.TextAlign.CENTER, color=ft.Colors.OUTLINE),
                ft.Divider(height=15, color=ft.Colors.BLACK26),
                url_textfield,
                ft.Row(
                    controls=[default_location_checkbox],
                    alignment=ft.MainAxisAlignment.START
                ),
                ft.Row(
                    controls=[custom_location_textfield],
                ),
                send_button,
                ft.Divider(height=15, color=ft.Colors.BLACK26),
               
            ],
            alignment=ft.MainAxisAlignment.START,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=10,
            expand=True,
            scroll=ft.ScrollMode.ADAPTIVE
        )
    )
    toggle_custom_location_field(None) # Set initial visibility

if __name__ == "__main__":
    if GOOGLE_SHEET_ID == 'YOUR_GOOGLE_SHEET_ID_HERE':
        print("\n--- ⚠️ CRITICAL SETUP WARNING (CONSOLE) ⚠️ ---")
        print("The GOOGLE_SHEET_ID is not set. Upload to Google Sheets will be SKIPPED.")
        print("Ensure 'credentials.json' is also present if GS upload is needed.")
        print("---------------------------------------------\n")

    if not os.path.exists(CREDENTIALS_FILE):
        print("\n--- SETUP WARNING (CONSOLE) ---")
        print(f"Google API credentials file ('{CREDENTIALS_FILE}') not found in script directory.")
        print("OAuth for Google Sheets may fail if this file is required by the GSheets functions.")
        print("----------------------------\n")
    
    print(f"User specified default download path: {USER_SPECIFIED_DEFAULT_DOWNLOAD_PATH}")
    ft.app(target=main)