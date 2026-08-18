"""
Capture notebook screenshots - convert .py to .ipynb, execute, export HTML, screenshot.
"""
import subprocess
import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright
import base64

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "notebooks"
SCREENSHOT_DIR = ROOT / "submission" / "screenshots"
SCREENSHOT_DIR.mkdir(exist_ok=True)

KERNEL = "lakehouse"

def jupytext_execute(py_file: Path) -> Path:
    """Convert .py to .ipynb and execute."""
    ipynb_file = NB_DIR / f"{py_file.stem}.ipynb"
    result = subprocess.run(
        [sys.executable, '-m', 'jupytext', '--to', 'notebook', 
         '--set-kernel', KERNEL, '--execute', 
         '--output', str(ipynb_file), str(py_file)],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"  jupytext error: {result.stderr[:500]}")
    return ipynb_file

def export_html(ipynb_file: Path) -> Path:
    """Export notebook to HTML."""
    html_file = SCREENSHOT_DIR / f"{ipynb_file.stem}.html"
    result = subprocess.run(
        [sys.executable, '-m', 'nbconvert', '--to', 'html', 
         '--output-dir', str(SCREENSHOT_DIR), str(ipynb_file)],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"  nbconvert error: {result.stderr[:500]}")
    return html_file

def capture_screenshot(html_file: Path, png_file: Path):
    """Capture full page screenshot using Playwright."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1400, 'height': 900})
        
        html_content = html_file.read_text(encoding='utf-8')
        encoded = base64.b64encode(html_content.encode('utf-8')).decode('ascii')
        data_url = f"data:text/html;base64,{encoded}"
        
        page.goto(data_url, wait_until='networkidle')
        time.sleep(0.5)
        
        page.screenshot(path=str(png_file), full_page=True)
        browser.close()
        print(f"  -> {png_file.name}")

def process_notebook(py_file: Path):
    """Process single notebook end-to-end."""
    print(f"\n[{py_file.name}]")
    
    # Step 1: jupytext execute
    print("  1. Executing notebook...")
    ipynb_file = jupytext_execute(py_file)
    
    # Step 2: nbconvert to HTML
    print("  2. Converting to HTML...")
    html_file = export_html(ipynb_file)
    
    # Step 3: screenshot
    print("  3. Capturing screenshot...")
    png_file = SCREENSHOT_DIR / f"{py_file.stem}.png"
    if html_file.exists():
        capture_screenshot(html_file, png_file)
    else:
        print(f"  ERROR: HTML not found")

def main():
    notebooks = sorted(
        p for p in NB_DIR.glob("*.py") 
        if not p.name.startswith("_")
    )
    
    print(f"=" * 60)
    print(f"Capturing {len(notebooks)} notebooks")
    print(f"Output: {SCREENSHOT_DIR}")
    print(f"=" * 60)
    
    for nb in notebooks:
        try:
            process_notebook(nb)
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'=' * 60}")
    print(f"Done! Screenshots saved to: {SCREENSHOT_DIR}")
    
    # List results
    png_files = list(SCREENSHOT_DIR.glob("*.png"))
    print(f"Captured: {len(png_files)}/{len(notebooks)}")
    for f in sorted(png_files):
        print(f"  {f.name}")

if __name__ == "__main__":
    main()
