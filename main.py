import os
import time
import requests
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
import logging
import re

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Undetected Chrome import
try:
    import undetected_chromedriver as uc
    UNDETECTED_AVAILABLE = True
except ImportError:
    UNDETECTED_AVAILABLE = False
    print("Warning: undetected-chromedriver not available. Install with: pip install undetected-chromedriver")

# Fallback imports
try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False
    print("Warning: webdriver-manager not available. Install with: pip install webdriver-manager")

class UniversalExcelScraper:
    def __init__(self, download_dir="./universal_downloads", headless=False, target_site=None):
        self.base_url = target_site or "https://www.jsda.or.jp/shiryoshitsu/toukei/finance/"
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.headless = headless
        self.driver = None
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('universal_scraper.log'), logging.StreamHandler()])
        self.logger = logging.getLogger(__name__)

    def setup_driver(self):
        try:
            if UNDETECTED_AVAILABLE and self._setup_undetected_chrome(): return True
            if WEBDRIVER_MANAGER_AVAILABLE and self._setup_regular_chrome_with_manager(): return True
            if self._setup_regular_chrome(): return True
            self.logger.error("All driver setup methods failed")
            return False
        except Exception as e:
            self.logger.error(f"Driver setup failed: {str(e)}")
            return False

    def _setup_undetected_chrome(self):
        try:
            options = uc.ChromeOptions()
            basic_args = ['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled']
            for arg in basic_args: options.add_argument(arg)
            if self.headless: options.add_argument('--headless=new')
            prefs = {"download.default_directory": str(self.download_dir.absolute()), "download.prompt_for_download": False}
            options.add_experimental_option("prefs", prefs)
            self.driver = uc.Chrome(options=options)
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            self.logger.info("Undetected Chrome initialized successfully")
            return True
        except Exception as e:
            self.logger.warning(f"Undetected Chrome failed: {e}")
            return False

    def _setup_regular_chrome_with_manager(self):
        try:
            from selenium.webdriver.chrome.options import Options as ChromeOptions
            from selenium.webdriver.chrome.service import Service
            chrome_options = ChromeOptions()
            basic_args = ['--no-sandbox', '--disable-dev-shm-usage']
            for arg in basic_args: chrome_options.add_argument(arg)
            if self.headless: chrome_options.add_argument('--headless')
            chrome_options.add_experimental_option("prefs", {"download.default_directory": str(self.download_dir.absolute()), "download.prompt_for_download": False})
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            self.logger.info("Regular Chrome with manager initialized successfully")
            return True
        except Exception as e:
            self.logger.warning(f"Regular Chrome with manager failed: {e}")
            return False

    def _setup_regular_chrome(self):
        try:
            from selenium.webdriver.chrome.options import Options as ChromeOptions
            chrome_options = ChromeOptions()
            basic_args = ['--no-sandbox', '--disable-dev-shm-usage']
            for arg in basic_args: chrome_options.add_argument(arg)
            if self.headless: chrome_options.add_argument('--headless')
            chrome_options.add_experimental_option("prefs", {"download.default_directory": str(self.download_dir.absolute()), "download.prompt_for_download": False})
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            self.logger.info("Regular Chrome initialized successfully")
            return True
        except Exception as e:
            self.logger.warning(f"Regular Chrome failed: {e}")
            return False
            
    def navigate_to_site(self):
        try:
            self.logger.info(f"Navigating to {self.base_url}")
            self.driver.get(self.base_url)
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            self.logger.info("Page loaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to navigate to site: {str(e)}")
            return False

    def find_download_links(self, first_match_only=True):
        try:
            self.logger.info("🔍 Searching for all potential Excel files on the page...")
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '.xls')]")))
            
            excel_links_elements = self._find_excel_links()
            if not excel_links_elements:
                self.logger.error("❌ No Excel links found!")
                return self._debug_and_fallback()

            valid_links = []
            for i, link_element in enumerate(excel_links_elements):
                link_info = self._process_excel_link(link_element, i, self.driver.current_url)
                if link_info:
                    valid_links.append(link_info)
            
            if not valid_links:
                self.logger.error("❌ No processable Excel links after validation!")
                return self._debug_and_fallback()
            
            self.logger.info(f"✅ Found {len(valid_links)} processable Excel links. Analyzing for best selection...")
            return self._select_best_files(valid_links, first_match_only)
        except Exception as e:
            self.logger.error(f"Excel link detection failed: {str(e)}", exc_info=True)
            return self._debug_and_fallback()

    def _find_excel_links(self):
        all_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, '.xls')]")
        self.logger.info(f"Found {len(all_links)} unique potential Excel links.")
        return all_links

    def _process_excel_link(self, link_element, position, base_url):
        try:
            href = link_element.get_attribute('href')
            if not href: return None
            
            full_url = urljoin(base_url, href)
            filename = os.path.basename(urlparse(full_url).path) or f"download_{position+1}.xls"
            
            context_text = filename + " " + full_url
            try:
                parent_container = link_element.find_element(By.XPATH, "./ancestor::tr[1] | ./ancestor::li[1]")
                context_text += " " + parent_container.text
            except NoSuchElementException:
                pass
                
            file_year = self._extract_year_from_text(context_text)
            is_sample = 'sample' in context_text.lower()
            
            return {'url': full_url, 'filename': filename, 'element': link_element, 'position': position, 'file_year': file_year or 0, 'is_sample': is_sample}
        except Exception:
            return None

    def _extract_year_from_text(self, text):
        if not text: return None
        years = re.findall(r'\b(20\d{2})\b', text)
        if years:
            valid_years = [int(y) for y in years if 2010 <= int(y) <= datetime.now().year + 2]
            if valid_years: return max(valid_years)
        return None

    def _select_best_files(self, valid_links, first_match_only):
        if not valid_links: return []

        valid_links.sort(key=lambda link: (-link['file_year'], link['position']))

        if not valid_links[0]['file_year']:
            self.logger.warning("⚠️ No year information found. Selecting based on page order only.")
            target_pool = [l for l in valid_links if not l['is_sample']] or valid_links
        else:
            target_year = valid_links[0]['file_year']
            self.logger.info(f"🎯 Top-most data section identified by Year: {target_year}. Focusing selection.")
            
            target_section_files = [l for l in valid_links if l['file_year'] == target_year]
            target_pool = [l for l in target_section_files if not l['is_sample']] or target_section_files
        
        if first_match_only:
            self.logger.info(f"Mode is 'first_match_only'. Selecting the top-ranked file from the pool.")
            self.logger.info(f"SELECTING: '{target_pool[0]['filename']}'")
            return target_pool[:1]
        else:
            self.logger.info(f"SELECTING: All {len(target_pool)} relevant files from the section.")
            return target_pool

    def download_selected_files(self, download_links):
        downloaded_files = []
        for link_info in download_links:
            if self.download_file_via_selenium(link_info):
                downloaded_files.append(link_info)
            else:
                self.logger.error(f"❌ Download failed for: {link_info['filename']}")
        return downloaded_files

    def download_file_via_selenium(self, link_info):
        try:
            self.logger.info(f"📥 Downloading '{link_info['filename']}' via Selenium click...")
            element = link_info['element']
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", element)
            time.sleep(1)
            
            # A more robust click
            ActionChains(self.driver).move_to_element(element).click().perform()
            
            expected_path = self.download_dir / link_info['filename']
            for _ in range(30): # 30 second timeout
                if expected_path.exists() and expected_path.stat().st_size > 0:
                    time.sleep(2) # Wait for file write to finish
                    self.logger.info(f"✅ Download complete: {link_info['filename']}")
                    return True
                time.sleep(1)
            self.logger.warning(f"⏰ Download timed out for: {link_info['filename']}")
            return False
        except Exception as e:
            self.logger.error(f"Selenium download failed for {link_info['filename']}: {e}")
            return False
            
    def run_scraper(self, first_match_only=True):
        try:
            self.logger.info("🚀 Starting Universal Excel Scraper...")
            mode_desc = "Download ONLY the FIRST file from the latest section" if first_match_only else "Download ALL files from the latest section"
            self.logger.info(f"Download mode: {mode_desc}")
            
            if not self.setup_driver() or not self.navigate_to_site(): return False
            
            download_links = self.find_download_links(first_match_only=first_match_only)
            if not download_links: return False
            
            downloaded_files_info = self.download_selected_files(download_links)
            
            if downloaded_files_info:
                self.logger.info(f"✅ Successfully downloaded {len(downloaded_files_info)} target file(s):")
                for info in downloaded_files_info:
                    self.logger.info(f"  - {info['filename']} (Year: {info['file_year']})")
                return True
            else:
                self.logger.error("❌ No files were successfully downloaded.")
                return False
        except Exception as e:
            self.logger.error(f"Scraper execution failed critically: {str(e)}", exc_info=True)
            return False
        finally:
            if self.driver:
                try: self.driver.quit(); self.logger.info("🔒 Browser closed.")
                except Exception: pass

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver: self.driver.quit()

def download_first_file_from_latest_section():
    """
    This is the specific function for your use case.
    It targets only the first Excel file from the newest publication section.
    """
    print("\n🎯 Running task: Download FIRST file from the LATEST section...")
    download_directory = "./latest_single_file"
    
    with UniversalExcelScraper(download_dir=download_directory, headless=True) as scraper:
        # The key is this parameter: first_match_only=True
        success = scraper.run_scraper(first_match_only=True) 
        
        if success:
            print(f"\n🎉 SUCCESS! The latest file was downloaded.")
            print(f"📁 Check the '{download_directory}' folder.")
        else:
            print(f"\n❌ FAILED! Check 'universal_scraper.log' for details.")

if __name__ == "__main__":
    # This will run the specific task you requested.
    download_first_file_from_latest_section()