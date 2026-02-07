import asyncio
import os
import re
import json
from pathlib import Path
from playwright.async_api import async_playwright
import aiohttp
import boto3
from dotenv import load_dotenv

load_dotenv()

HISTORY_FILE = "download_history.txt"
PROGRESS_FILE = "progress.json"
FAILED_FILE = "failed_downloads.txt"

def load_processed_laws():
    """Load set of already processed law names/filenames"""
    if not os.path.exists(HISTORY_FILE):
        return set()
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    except Exception as e:
        print(f"Error loading processed laws: {e}")
        return set()

def save_processed_law(name):
    """Append a processed law to the history file"""
    try:
        with open(HISTORY_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{name}\n")
    except Exception as e:
        print(f"Error saving processed law: {e}")

def save_failed_law(name, reason=""):
    """Append a failed law to the failed downloads file"""
    try:
        with open(FAILED_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{name}|{reason}\n")
    except Exception as e:
        print(f"Error saving failed law: {e}")

def load_progress():
    """Load progress from JSON file for resume functionality"""
    if not os.path.exists(PROGRESS_FILE):
        return None
    try:
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading progress: {e}")
        return None

def update_progress(total, processed, current_page, current_row=0, skipped_repealed=0):
    """Update progress JSON file with current state"""
    status = {
        "total_laws_approx": total,
        "processed_count": processed,
        "skipped_repealed": skipped_repealed,
        "files_left_approx": max(0, total - processed - skipped_repealed),
        "current_page": current_page,
        "current_row": current_row,
        "last_updated": "running"
    }
    try:
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2)
    except Exception as e:
        print(f"Error updating progress: {e}")

def load_repealed_laws():
    """Load repealed law names from JSON file"""
    try:
        with open('repealed_law_names.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get('names', []))
    except FileNotFoundError:
        print("Warning: repealed_law_names.json not found")
        return set()
    except Exception as e:
        print(f"Error loading repealed laws: {e}")
        return set()

def is_repealed(law_name, repealed_names):
    """Check if law is in repealed list"""
    norm_name = " ".join(law_name.lower().split())
    
    for repealed in repealed_names:
        norm_repealed = " ".join(repealed.lower().split())
        if norm_repealed.startswith(norm_name):
             # Ensure we matched a complete name/phrase
             if len(norm_repealed) == len(norm_name) or norm_repealed[len(norm_name)] in [',', ' ', '(']:
                 return True
    return False

def upload_to_s3(file_path, bucket_name, object_name=None):
    """Upload file to S3 bucket"""
    if object_name is None:
        object_name = os.path.basename(file_path)

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        
        s3_client.upload_file(str(file_path), bucket_name, object_name)
        print(f"  ✓ Uploaded to S3: {object_name}")
        return True
    except Exception as e:
        print(f"  ✗ Failed to upload to S3: {e}")
        return False


async def download_pdf(session, url, filename, download_dir, headers=None):
    """Download PDF file using aiohttp with retry logic"""
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.indiacode.nic.in/'
        }
    
    try:
        filepath = download_dir / filename
        # Skip if already downloaded
        if filepath.exists():
            print(f"  ✓ Already exists: {filename}")
            return True
        
        # Retry logic for downloads
        for attempt in range(3):
            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status == 200:
                        content = await response.read()
                        # Verify it's actually a PDF
                        if len(content) > 100 and (content[:4] == b'%PDF' or b'%PDF' in content[:1024]):
                            with open(filepath, 'wb') as f:
                                f.write(content)
                            print(f"  ✓ Downloaded: {filename}")
                            return True
                        else:
                            print(f"  ✗ Not a valid PDF: {filename}")
                            return False
                    elif response.status == 404:
                        return False  # No retry for 404
                    else:
                        print(f"  ✗ Status {response.status}, attempt {attempt + 1}/3")
                        if attempt < 2:
                            await asyncio.sleep(2)
            except asyncio.TimeoutError:
                print(f"  ✗ Timeout on attempt {attempt + 1}/3")
                if attempt < 2:
                    await asyncio.sleep(2)
            except Exception as e:
                print(f"  ✗ Error on attempt {attempt + 1}/3: {e}")
                if attempt < 2:
                    await asyncio.sleep(2)
        
        print(f"  ✗ Failed to download {filename} after 3 attempts")
        return False
    except Exception as e:
        print(f"  ✗ Error downloading {filename}: {e}")
        return False


def sanitize_filename(filename):
    """Sanitize filename to remove invalid characters"""
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    if len(filename) > 200:
        filename = filename[:200]
    return filename


async def navigate_to_page(page, target_page):
    """Navigate to a specific page number in the table"""
    if target_page <= 1:
        return True
    
    print(f"Resuming: Navigating to page {target_page}...")
    
    # Method 1: Try using DataTables API directly via JavaScript
    try:
        print(f"  Trying DataTables API to jump to page {target_page}...")
        result = await page.evaluate(f"""
            () => {{
                try {{
                    const table = $('#myTableSection').DataTable();
                    if (table) {{
                        table.page({target_page - 1}).draw('page');
                        return true;
                    }}
                }} catch (e) {{
                    return false;
                }}
                return false;
            }}
        """)
        if result:
            await page.wait_for_timeout(3000)
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass
            print(f"  ✓ Successfully jumped to page {target_page} via DataTables API")
            return True
        else:
            print(f"  DataTables API method failed, trying alternatives...")
    except Exception as e:
        print(f"  DataTables API error: {e}, trying alternatives...")
    
    # Method 2: Try using page input if available
    try:
        page_input = await page.query_selector("#myTableSection_paginate input[type='text']")
        if not page_input:
            # Try alternative selectors
            selectors = [
                ".dataTables_paginate input",
                "input.paginate_input",
                "#myTableSection_wrapper input[type='text']"
            ]
            for selector in selectors:
                page_input = await page.query_selector(selector)
                if page_input:
                    break
        
        if page_input:
            print(f"  Using page input to jump to page {target_page}...")
            await page_input.click()
            await page_input.fill("")
            await page_input.type(str(target_page))
            await page_input.press("Enter")
            await page.wait_for_timeout(3000)
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass
            print(f"  ✓ Successfully jumped to page {target_page} via page input")
            return True
    except Exception as e:
        print(f"  Page input method error: {e}")
    
    # Method 3: Last resort - click next button repeatedly (very slow!)
    print(f"  WARNING: Using slow page-by-page navigation. This will take a while...")
    current_page = 1
    while current_page < target_page:
        next_button = await page.query_selector("#myTableSection_next:not(.disabled)")
        if not next_button:
            print(f"  Could not navigate to page {target_page}, stopped at page {current_page}")
            return False
        
        await next_button.click()
        await page.wait_for_timeout(1500)
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            pass
        current_page += 1
        
        if current_page % 10 == 0:
            print(f"  Navigating... currently at page {current_page}")
    
    print(f"  Reached page {target_page}")
    return True


async def scrape_india_code(max_retries=3):
    """
    Crawl India Code website for NEW files only:
    1. Navigate to the website and perform search
    2. Check each law against download history and repealed laws
    3. Download ONLY if: NOT already downloaded AND NOT in repealed list
    4. Optimize by skipping ahead when encountering many already-processed files
    """
    download_dir = Path("indian_laws_pdfs")
    download_dir.mkdir(exist_ok=True)
    
    repealed_names = load_repealed_laws()
    print(f"\n{'='*60}")
    print("CRAWLER MODE - Checking for NEW files only")
    print(f"{'='*60}")
    print(f"Loaded {len(repealed_names)} repealed laws to skip.")

    processed_files = load_processed_laws()
    print(f"Loaded {len(processed_files)} previously downloaded files.")
    print(f"Will only download files that are:")
    print(f"  ✓ NOT in download history")
    print(f"  ✓ NOT in repealed laws list")
    print(f"{'='*60}\n")
    
    # Crawler always starts from page 1 to check for new files
    resume_page = 1
    resume_row = 0
    
    async with async_playwright() as p:
        # Try Firefox as fallback - often works better on government sites
        browser_type = os.getenv('BROWSER_TYPE', 'firefox')
        
        if browser_type == 'firefox':
            print("Using Firefox browser...")
            browser = await p.firefox.launch(
                headless=False,
                args=['--no-sandbox']
            )
        else:
            print("Using Chromium browser...")
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--disable-gpu',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--allow-running-insecure-content'
                ]
            )
        
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            java_script_enabled=True,
            ignore_https_errors=True
        )
        
        # Set extra headers to look more like a real browser
        await context.set_extra_http_headers({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })
        
        page = await context.new_page()
        
        # Remove webdriver detection
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            window.chrome = { runtime: {} };
        """)

        try:
            # Try multiple approaches to get to the acts list
            search_success = False
            
            # Direct URLs that might work (bypass search form)
            direct_urls = []
            
            for attempt in range(1, max_retries + 1):
                # First try direct URLs
                if attempt == 1:
                    for direct_url in direct_urls:
                        try:
                            print(f"Trying direct URL: {direct_url}")
                            await page.goto(direct_url, wait_until="domcontentloaded", timeout=60000)
                            await page.wait_for_timeout(5000)
                            
                            # Check if table is present
                            table = await page.query_selector("#myTableSection, table.dataTable, .dataTables_wrapper")
                            if table:
                                print("  Found table via direct URL!")
                                search_success = True
                                break
                        except Exception as e:
                            print(f"  Direct URL failed: {e}")
                            continue
                    
                    if search_success:
                        break
                
                # Fall back to homepage search
                try:
                    print(f"Trying homepage search... (attempt {attempt}/{max_retries})")
                    await page.goto("https://www.indiacode.nic.in/", wait_until="domcontentloaded", timeout=60000)
                    
                    # Wait longer for JavaScript to render
                    await page.wait_for_timeout(5000)
                    
                    # Try to wait for page to be fully interactive
                    try:
                        await page.wait_for_function("document.readyState === 'complete'", timeout=30000)
                    except Exception:
                        pass
                    
                    # Additional wait for dynamic content
                    await page.wait_for_timeout(3000)
                    
                    print("Waiting for search input to load...")
                    
                    # Try multiple possible selectors including iframes
                    search_selectors = [
                        "#tequery",
                        "input[name='tequery']",
                        "input[type='text'][id='tequery']",
                        "input[placeholder*='search' i]",
                        "input[placeholder*='Search' i]",
                        "#searchquery",
                        "input.form-control",
                        "input[type='search']"
                    ]
                    search_input = None
                    
                    for selector in search_selectors:
                        try:
                            elem = await page.query_selector(selector)
                            if elem:
                                is_visible = await elem.is_visible()
                                if is_visible:
                                    search_input = elem
                                    print(f"  Found search input with selector: {selector}")
                                    break
                        except Exception:
                            continue
                    
                    # Check if there's an iframe we need to switch to
                    if not search_input:
                        frames = page.frames
                        for frame in frames:
                            if frame != page.main_frame:
                                try:
                                    for selector in search_selectors:
                                        elem = await frame.query_selector(selector)
                                        if elem:
                                            print(f"  Found search input in iframe with selector: {selector}")
                                            # Switch context to work with iframe
                                            search_input = elem
                                            break
                                    if search_input:
                                        break
                                except Exception:
                                    continue
                    
                    if not search_input:
                        # Save page content for debugging
                        page_content = await page.content()
                        with open(f"debug_page_content_attempt_{attempt}.html", "w", encoding="utf-8") as f:
                            f.write(page_content)
                        await page.screenshot(path=f"debug_screenshot_attempt_{attempt}.png", full_page=True)
                        
                        # Log what elements we can find
                        all_inputs = await page.query_selector_all("input")
                        print(f"  Found {len(all_inputs)} input elements on page")
                        for i, inp in enumerate(all_inputs[:10]):
                            inp_id = await inp.get_attribute("id")
                            inp_name = await inp.get_attribute("name")
                            inp_type = await inp.get_attribute("type")
                            print(f"    Input {i}: id={inp_id}, name={inp_name}, type={inp_type}")
                        
                        raise Exception("Search input not found after trying all selectors")
                    
                    # Proceed with search
                    print("Disabling 'required' attribute on search input...")
                    await page.evaluate("""
                        () => {
                            const inputs = document.querySelectorAll('input');
                            inputs.forEach(input => {
                                input.removeAttribute('required');
                                input.removeAttribute('minlength');
                            });
                        }
                    """)
                    
                    print("Clicking on 'All' radio button...")
                    radio_selectors = ["input[name='searchradio'][value='all']", "#all", "input[value='all']", "input[type='radio']"]
                    for selector in radio_selectors:
                        try:
                            radio_btn = await page.query_selector(selector)
                            if radio_btn:
                                await radio_btn.click()
                                print(f"  Clicked radio with selector: {selector}")
                                break
                        except Exception:
                            continue
                    
                    await page.wait_for_timeout(1000)
                    
                    print("Clicking on 'Go!' search button...")
                    search_btn_selectors = ["#btngo", "button[type='submit']", "input[type='submit']", ".btn-search", "button.btn", "#btnsearch"]
                    for selector in search_btn_selectors:
                        try:
                            btn = await page.query_selector(selector)
                            if btn:
                                await btn.click()
                                print(f"  Clicked search button with selector: {selector}")
                                search_success = True
                                break
                        except Exception:
                            continue
                    
                    if search_success:
                        break
                    
                except Exception as e:
                    print(f"  Attempt {attempt} failed: {e}")
                    if attempt < max_retries:
                        wait_time = 15 * attempt
                        print(f"  Retrying in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        raise Exception(f"Failed to load page after {max_retries} attempts: {e}")
            
            if not search_success:
                raise Exception("Could not access the acts list via any method")
            
            print("Waiting for search results...")
            try:
                await page.wait_for_load_state("networkidle", timeout=90000)
            except Exception:
                print("  Warning: networkidle timeout, continuing anyway...")
            
            # Extra wait for dynamic content
            await page.wait_for_timeout(5000)
            
            print("Waiting for table '#myTableSection' to be visible...")
            # Try multiple table selectors
            table_selectors = ["#myTableSection", "table.dataTable", "#myTableSection_wrapper", "table[id*='Table']"]
            table_found = False
            for selector in table_selectors:
                try:
                    await page.wait_for_selector(selector, state="visible", timeout=60000)
                    table_found = True
                    print(f"  Found table with selector: {selector}")
                    break
                except Exception:
                    continue
            
            if not table_found:
                # Save debug info
                page_content = await page.content()
                with open("debug_table_not_found.html", "w", encoding="utf-8") as f:
                    f.write(page_content)
                await page.screenshot(path="debug_table_not_found.png")
                raise Exception("Table not found after search")
            
            print("Table loaded successfully!")
            
            info_text = await page.inner_text("#myTableSection_info")
            print(f"\nTable info: {info_text}")
            
            total_laws_count = 0
            try:
                match = re.search(r'of\s+([\d,]+)\s+entries', info_text)
                if match:
                    total_laws_count = int(match.group(1).replace(',', ''))
            except Exception:
                pass

            # Crawler starts from page 1
            async with aiohttp.ClientSession() as session:
                page_num = 1
                total_downloaded = 0  # Count only NEW downloads in this session
                skipped_repealed = 0
                skipped_already_downloaded = 0
                consecutive_empty_pages = 0  # Track pages with no valid links
                consecutive_skipped = 0  # Track consecutive already-downloaded files
                
                while True:
                    print(f"\n{'='*60}")
                    print(f"Processing Page {page_num}")
                    print(f"{'='*60}")
                    
                    # Wait for table to be properly loaded after navigation
                    await page.wait_for_timeout(1000)
                    await page.wait_for_selector("#myTableSection tbody tr", timeout=60000)
                    rows = await page.query_selector_all("#myTableSection tbody tr")
                    print(f"Found {len(rows)} rows on this page")
                    
                    links_found_on_page = 0
                    new_downloads_on_page = 0
                    
                    for idx, row in enumerate(rows):
                        
                        row_num = idx + 1
                        
                        try:
                            # Try multiple selectors for the law link
                            link_elem = await row.query_selector("a.allacts")
                            if not link_elem:
                                # Fallback: try any anchor tag in the row
                                link_elem = await row.query_selector("td a[href*='/handle/']")
                            if not link_elem:
                                link_elem = await row.query_selector("td:first-child a")
                            if not link_elem:
                                link_elem = await row.query_selector("a[href]")
                            
                            if not link_elem:
                                print(f"  Row {row_num}: No link found, skipping...")
                                continue
                            
                            links_found_on_page += 1
                            
                            law_name = await link_elem.inner_text()
                            print(f"\n  Row {row_num}: {law_name}")
                            
                            # Create filename early for duplicate check
                            pdf_filename = sanitize_filename(f"{law_name}.pdf")
                            
                            # CRAWLER LOGIC: Skip if already downloaded
                            if pdf_filename in processed_files:
                                print(f"    ⏭️  SKIP: Already downloaded")
                                skipped_already_downloaded += 1
                                consecutive_skipped += 1
                                update_progress(total_laws_count, len(processed_files) + total_downloaded, page_num, idx, skipped_repealed)
                                
                                # Optimization: If we've skipped 20+ consecutive files, jump ahead
                                if consecutive_skipped >= 20:
                                    print(f"\n    {'⚡'*20}")
                                    print(f"    ⚡ OPTIMIZATION: {consecutive_skipped} consecutive files already downloaded")
                                    print(f"    ⚡ Likely all files on this page are old - moving to next page")
                                    print(f"    {'⚡'*20}\n")
                                    break  # Skip rest of this page
                                
                                continue
                            
                            # CRAWLER LOGIC: Skip if repealed
                            if is_repealed(law_name, repealed_names):
                                print(f"    ⏭️  SKIP: Repealed law")
                                skipped_repealed += 1
                                consecutive_skipped += 1
                                update_progress(total_laws_count, len(processed_files) + total_downloaded, page_num, idx, skipped_repealed)
                                continue
                            
                            # Reset consecutive skip counter - found a new file
                            consecutive_skipped = 0
                            print(f"    🆕 NEW FILE DETECTED - Downloading...")
                            
                            href = await link_elem.get_attribute("href")
                            if not href:
                                print(f"    ✗ No href found")
                                continue
                            
                            detail_url = f"https://www.indiacode.nic.in{href}"
                            
                            # Retry logic for detail page
                            pdf_downloaded = False
                            for detail_attempt in range(2):
                                detail_page = await context.new_page()
                                
                                try:
                                    await detail_page.goto(detail_url, wait_until="domcontentloaded", timeout=45000)
                                    await detail_page.wait_for_timeout(2000)
                                    
                                    # Get ALL PDF links and try them in order
                                    pdf_links = await detail_page.query_selector_all("a[href*='.pdf']")
                                    
                                    if pdf_links:
                                        print(f"    Found {len(pdf_links)} PDF link(s)")
                                        
                                        for pdf_idx, pdf_link in enumerate(pdf_links):
                                            pdf_href = await pdf_link.get_attribute("href")
                                            
                                            if pdf_href:
                                                # Construct full PDF URL
                                                if pdf_href.startswith("http"):
                                                    pdf_url = pdf_href
                                                else:
                                                    pdf_url = f"https://www.indiacode.nic.in{pdf_href}"
                                                
                                                print(f"    Trying PDF {pdf_idx + 1}/{len(pdf_links)}: {pdf_url[:80]}...")
                                                if await download_pdf(session, pdf_url, pdf_filename, download_dir):
                                                    local_path = download_dir / pdf_filename
                                                    
                                                    if upload_to_s3(local_path, "ragbareacttestings3vector", pdf_filename):
                                                        try:
                                                            os.remove(local_path)
                                                            print(f"  ✓ Deleted local file: {pdf_filename}")
                                                            
                                                            processed_files.add(pdf_filename)
                                                            save_processed_law(pdf_filename)
                                                            total_downloaded += 1
                                                            new_downloads_on_page += 1
                                                            
                                                            update_progress(total_laws_count, len(processed_files) + total_downloaded, page_num, idx, skipped_repealed)
                                                            pdf_downloaded = True
                                                            print(f"  🎉 SUCCESS: New file downloaded and uploaded!")
                                                            
                                                        except OSError as e:
                                                            print(f"  Warning: Could not delete {pdf_filename}: {e}")
                                                            pdf_downloaded = True
                                                    else:
                                                        print(f"  ✗ Upload failed, keeping local file: {pdf_filename}")
                                                        pdf_downloaded = True
                                                    
                                                    break  # Successfully downloaded, exit PDF loop
                                        
                                        if pdf_downloaded:
                                            break  # Exit detail attempt loop
                                        else:
                                            print(f"    ✗ All {len(pdf_links)} PDF links failed")
                                            save_failed_law(law_name, f"All {len(pdf_links)} PDF links returned 404")
                                    else:
                                        # Try to find PDF links with different selectors
                                        alt_selectors = [
                                            "a[href$='.pdf']",
                                            "a[title*='PDF']",
                                            "a[title*='pdf']",
                                            "a[href*='bitstream']"
                                        ]
                                        found_alt = False
                                        for alt_sel in alt_selectors:
                                            alt_links = await detail_page.query_selector_all(alt_sel)
                                            if alt_links:
                                                print(f"    Found {len(alt_links)} links with {alt_sel}")
                                                found_alt = True
                                                break
                                        
                                        if not found_alt:
                                            print(f"    ✗ No PDF links found on detail page")
                                            save_failed_law(law_name, "No PDF links found")
                                    
                                    break  # Exit retry loop if page loaded successfully
                                        
                                except Exception as e:
                                    print(f"    ✗ Detail page error (attempt {detail_attempt + 1}/2): {str(e)[:60]}")
                                    if detail_attempt == 1:
                                        save_failed_law(law_name, f"Detail page error: {str(e)[:50]}")
                                    if detail_attempt < 1:
                                        await asyncio.sleep(3)
                                finally:
                                    await detail_page.close()
                            
                            await page.wait_for_timeout(300)
                            
                        except Exception as e:
                            print(f"  ✗ Error processing row {row_num}: {e}")
                            continue
                    
                    # Page summary
                    print(f"\n{'─'*60}")
                    print(f"📊 Page {page_num} Summary:")
                    print(f"   • New downloads: {new_downloads_on_page}")
                    print(f"   • Already downloaded: {skipped_already_downloaded}")
                    print(f"   • Repealed (skipped): {skipped_repealed}")
                    print(f"{'─'*60}\n")
                    
                    # Track consecutive pages with no valid links
                    if links_found_on_page == 0:
                        consecutive_empty_pages += 1
                        print(f"  ⚠️  WARNING: No valid links found on this page ({consecutive_empty_pages} consecutive empty pages)")
                        
                        if consecutive_empty_pages >= 10:
                            print(f"\n{'='*60}")
                            print("❌ ERROR: 10 consecutive pages with no links found!")
                            print("Likely navigated beyond actual data. Stopping crawler.")
                            print(f"{'='*60}")
                            break
                    else:
                        consecutive_empty_pages = 0  # Reset counter when we find links
                    
                    # Check for next page
                    next_button = await page.query_selector("#myTableSection_next")
                    if next_button:
                        classes = await next_button.get_attribute("class")
                        if "disabled" in classes:
                            print(f"\n{'='*60}")
                            print("Reached last page!")
                            print(f"Total PDFs downloaded: {total_downloaded}")
                            print(f"{'='*60}")
                            break
                        else:
                            # Try DataTables API for direct page navigation first
                            next_page = page_num + 1
                            direct_nav_success = False
                            
                            try:
                                result = await page.evaluate(f"""
                                    () => {{
                                        try {{
                                            const table = $('#myTableSection').DataTable();
                                            if (table) {{
                                                table.page({next_page - 1}).draw('page');
                                                return true;
                                            }}
                                        }} catch (e) {{
                                            return false;
                                        }}
                                        return false;
                                    }}
                                """)
                                if result:
                                    await page.wait_for_timeout(2000)
                                    try:
                                        await page.wait_for_load_state("networkidle", timeout=30000)
                                    except Exception:
                                        pass
                                    print(f"\n[NAV] Page {page_num} -> {next_page} via DataTables API ✓")
                                    direct_nav_success = True
                            except Exception as e:
                                print(f"\n[NAV] DataTables API failed: {e}")
                            
                            # Fallback to clicking next button
                            if not direct_nav_success:
                                print(f"\n[NAV] Page {page_num} -> {next_page} via CLICK NEXT (fallback)")
                                await next_button.click()
                                await page.wait_for_timeout(2000)
                                try:
                                    await page.wait_for_load_state("networkidle", timeout=30000)
                                except Exception:
                                    pass
                            
                            page_num += 1
                    else:
                        print("No next button found, stopping...")
                        break
            
            print(f"\n{'='*60}")
            print(f"✅ CRAWLER SESSION COMPLETED!")
            print(f"{'='*60}")
            print(f"📥 NEW files downloaded this session: {total_downloaded}")
            print(f"⏭️  Already downloaded (skipped): {skipped_already_downloaded}")
            print(f"⏭️  Repealed laws (skipped): {skipped_repealed}")
            print(f"📁 Total files in history: {len(processed_files)}")
            print(f"💾 PDFs saved in: {download_dir.absolute()}")
            print(f"{'='*60}\n")
            
            # Mark progress as complete
            update_progress(total_laws_count, len(processed_files) + total_downloaded, page_num, 0, skipped_repealed)

        except Exception as e:
            print(f"\n✗ Error occurred: {e}")
            # Take screenshot on error
            try:
                await page.screenshot(path="error_screenshot.png", timeout=10000)
                print("  Saved error screenshot to error_screenshot.png")
            except Exception as screenshot_error:
                print(f"  Failed to capture screenshot: {screenshot_error}")
            # Save page content on error
            try:
                page_content = await page.content()
                with open("error_page_content.html", "w", encoding="utf-8") as f:
                    f.write(page_content)
                print("  Saved page content to error_page_content.html")
            except Exception as content_error:
                print(f"  Failed to save page content: {content_error}")
            raise

        finally:
            await browser.close()
            print("\nBrowser closed.")


if __name__ == "__main__":
    import sys
    
    # Print usage info
    print("=" * 60)
    print("Indian Law Crawler - NEW Files Only")
    print("=" * 60)
    print("This crawler checks for NEW files on the website.")
    print("It will SKIP files that are:")
    print("  • Already downloaded (in download_history.txt)")
    print("  • Listed as repealed (in repealed_law_names.json)")
    print("=")
    print("Environment variables:")
    print("  BROWSER_TYPE: 'chromium' (default) or 'firefox'")
    print("  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION: S3 config")
    print("=" * 60)
    
    asyncio.run(scrape_india_code())
