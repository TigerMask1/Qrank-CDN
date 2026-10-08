"""
AIRstudy / QRank — Heavy 500+ Page Batch PDF Worker.
Runs on GitHub Actions (7GB RAM runner) to extract, crop, calibrate, and store questions
from large books, test series, and module PDFs into Backblaze B2 & Turso DB.
Uses QRank's exact extraction prompts, schemas, and second-look recovery.
Optimized for concurrent parallel runs with per-request timeouts and rate-limit backoff.
"""
import os
import io
import sys
import json
import time
import random
import argparse
import requests
from PIL import Image

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import pymupdf as fitz
from google import genai
from google.genai import types
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess

from b2_storage import upload_question_image_b2
from turso_db import execute_query, init_turso_schema
from taxonomy_engine import evolve_taxonomy_path, warm_taxonomy_cache
from qmatch_service import generate_embedding

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
GEMINI_FALLBACK_MODEL = os.getenv("GEMINI_FALLBACK_MODEL", "gemini-3.1-flash-lite")
GEMINI_API_KEYS = [k.strip() for k in os.getenv("GEMINI_API_KEYS", "").split(",") if k.strip()]
if not GEMINI_API_KEYS and os.getenv("GEMINI_API_KEY"):
    GEMINI_API_KEYS = [os.getenv("GEMINI_API_KEY").strip()]

_key_idx = 0

def get_gemini_client():
    global _key_idx
    if not GEMINI_API_KEYS:
        return None
    key = GEMINI_API_KEYS[_key_idx % len(GEMINI_API_KEYS)]
    _key_idx += 1
    try:
        # Pass 35s timeout to avoid socket hangs
        return genai.Client(api_key=key, http_options=types.HttpOptions(timeout=35000))
    except Exception:
        return genai.Client(api_key=key)

def parse_args():
    parser = argparse.ArgumentParser(description="AIRstudy 500+ Page Batch Worker")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--file-url", default="")
    parser.add_argument("--pdf-url", default="")
    parser.add_argument("--file-path", default="")
    parser.add_argument("--total-pages", default="500")
    parser.add_argument("--start-page", default="0")
    parser.add_argument("--chunk-size", default="150")
    parser.add_argument("--branch", default=os.getenv("GITHUB_REF_NAME", "feature/air-study-qmatch-upgrade"))
    parser.add_argument("--auto-chain", default="true")
    parser.add_argument("--delete-on-complete", default="false")
    parser.add_argument("--callback-url", default=os.getenv("CALLBACK_URL", ""))
    parser.add_argument("--callback-secret", default=os.getenv("CALLBACK_SECRET", ""))
    return parser.parse_args()

def crop_bbox(img: Image.Image, bbox_norm: any, pad_px: int = 10) -> Image.Image:
    w, h = img.size
    ymin, xmin, ymax, xmax = 0.0, 0.0, 1000.0, 1000.0
    try:
        if isinstance(bbox_norm, (list, tuple)):
            while len(bbox_norm) == 1 and isinstance(bbox_norm[0], (list, tuple)):
                bbox_norm = bbox_norm[0]
            if len(bbox_norm) >= 4:
                ymin, xmin, ymax, xmax = float(bbox_norm[0]), float(bbox_norm[1]), float(bbox_norm[2]), float(bbox_norm[3])
        elif isinstance(bbox_norm, dict):
            if "box_2d" in bbox_norm and isinstance(bbox_norm["box_2d"], (list, tuple)) and len(bbox_norm["box_2d"]) >= 4:
                ymin, xmin, ymax, xmax = [float(x) for x in bbox_norm["box_2d"][:4]]
            else:
                ymin = float(bbox_norm.get("ymin", bbox_norm.get("y0", bbox_norm.get("top", 0))))
                xmin = float(bbox_norm.get("xmin", bbox_norm.get("x0", bbox_norm.get("left", 0))))
                ymax = float(bbox_norm.get("ymax", bbox_norm.get("y1", bbox_norm.get("bottom", 1000))))
                xmax = float(bbox_norm.get("xmax", bbox_norm.get("x1", bbox_norm.get("right", 1000))))
    except Exception as e:
        print(f"[Warning] Could not parse bbox {bbox_norm}: {e}. Defaulting to full image.")
        ymin, xmin, ymax, xmax = 0.0, 0.0, 1000.0, 1000.0

    ymin, ymax = max(0.0, min(ymin, ymax)), min(1000.0, max(ymin, ymax))
    xmin, xmax = max(0.0, min(xmin, xmax)), min(1000.0, max(xmin, xmax))
    if ymax - ymin < 5:
        ymin, ymax = 0.0, 1000.0
    if xmax - xmin < 5:
        xmin, xmax = 0.0, 1000.0

    x0 = max(0, int((xmin / 1000.0) * w - pad_px))
    y0 = max(0, int((ymin / 1000.0) * h - pad_px))
    x1 = min(w, int((xmax / 1000.0) * w + pad_px))
    y1 = min(h, int((ymax / 1000.0) * h + pad_px))
    return img.crop((x0, y0, x1, y1))

def post_callback(callback_url: str, secret: str, payload: dict):
    if not callback_url:
        return
    try:
        url = f"{callback_url.rstrip('/')}/api/batch-pdf/callback"
        requests.post(url, json=payload, headers={"X-Callback-Secret": secret}, timeout=25)
    except Exception as e:
        print(f"[Callback Error]: {e}")

def infer_subject_from_filename(name: str) -> str:
    n = name.lower()
    if any(k in n for k in ["bio", "botany", "zoology"]):
        return "Biology"
    if any(k in n for k in ["chem", "organic", "inorganic", "physical chemistry"]):
        return "Chemistry"
    if any(k in n for k in ["math", "calculus", "algebra", "geometry"]):
        return "Mathematics"
    return "Physics"

def resolve_lfs_pointer_if_needed(file_path: str):
    if not file_path or not os.path.exists(file_path):
        return
    try:
        if os.path.getsize(file_path) < 1000:
            with open(file_path, "r", errors="ignore") as f:
                header = f.read(100)
            if "version https://git-lfs.github.com" in header:
                print(f"[LFS] Detected Git LFS pointer for {file_path}. Fetching actual binary via git lfs pull...")
                token = os.getenv("GITHUB_TOKEN") or os.getenv("PRIVATE_REPO_PAT")
                if token:
                    subprocess.run(["git", "remote", "set-url", "origin", f"https://{token}@github.com/TigerMask1/Qrank.git"], check=False)
                subprocess.run(["git", "lfs", "pull", "--include", file_path], check=False)
    except Exception as e:
        print(f"[LFS Check Error]: {e}")

def delete_processed_pdf(pdf_path: str, branch: str = "feature/air-study-qmatch-upgrade"):
    """Deletes fully processed PDF textbook from git repository."""
    normalized = os.path.normpath(pdf_path).replace("\\", "/")
    if "paper_pdfs" in normalized or "/papers/" in normalized or "qrank_" in normalized:
        print(f"[Auto-Cleanup Skipped] Protected QRank project file: {pdf_path}. Will NOT be deleted.")
        return

    token = os.getenv("GITHUB_TOKEN") or os.getenv("PRIVATE_REPO_PAT")
    repo = os.getenv("GITHUB_REPO", "TigerMask1/Qrank")
    filename = os.path.basename(pdf_path)
    print(f"[Auto-Cleanup] Deleting completed PDF from repository: {filename}...")
    try:
        if os.path.exists(pdf_path):
            subprocess.run(["git", "config", "user.name", "airstudy-bot"], check=False)
            subprocess.run(["git", "config", "user.email", "bot@airstudy.ai"], check=False)
            subprocess.run(["git", "rm", "-f", pdf_path], check=False)
            commit_res = subprocess.run(["git", "commit", "-m", f"chore(batch): delete completed book {filename} [skip ci]"], check=False)
            if commit_res.returncode == 0:
                if token:
                    push_url = f"https://{token}@github.com/{repo}.git"
                    subprocess.run(["git", "push", push_url, f"HEAD:{branch}"], check=False)
                else:
                    subprocess.run(["git", "push", "origin", branch], check=False)
                print(f"[Auto-Cleanup] Successfully removed and committed {filename} from git branch {branch}")
                return
    except Exception as e:
        print(f"[Auto-Cleanup Git Error] {e}")

    if token and repo:
        try:
            rel_path = os.path.relpath(pdf_path, os.getcwd()) if os.path.isabs(pdf_path) else pdf_path
            headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
            get_url = f"https://api.github.com/repos/{repo}/contents/{rel_path}?ref={branch}"
            get_res = requests.get(get_url, headers=headers, timeout=15)
            if get_res.status_code == 200:
                sha = get_res.json().get("sha")
                del_url = f"https://api.github.com/repos/{repo}/contents/{rel_path}"
                del_payload = {
                    "message": f"chore(batch): delete completed book {filename} [skip ci]",
                    "sha": sha,
                    "branch": branch
                }
                del_res = requests.delete(del_url, headers=headers, json=del_payload, timeout=20)
                if del_res.status_code in [200, 204]:
                    print(f"[Auto-Cleanup API] Successfully deleted {rel_path} via GitHub API.")
        except Exception as e:
            print(f"[Auto-Cleanup API Error] {e}")

# Maintain backward compatibility alias
auto_delete_book_from_git = delete_processed_pdf

def process_single_pdf(file_target: str, args):
    print(f"\n========================================================")
    print(f"=== Starting AIRstudy Batch Ingestion for: {file_target} ===")
    print(f"========================================================")
    client = get_gemini_client()

    doc = None
    if os.path.exists(file_target):
        resolve_lfs_pointer_if_needed(file_target)
        doc = fitz.open(file_target)
    else:
        print(f"Downloading remote PDF from {file_target}...")
        resp = requests.get(file_target, stream=True, timeout=30)
        doc = fitz.open(stream=resp.content, filetype="pdf")

    if not doc:
        print(f"Failed to open PDF document: {file_target}")
        return 0

    doc_len = len(doc)
    start_p = max(0, int(args.start_page or 0))
    chunk_sz = int(args.chunk_size or 150)
    max_p = int(args.total_pages or 500)
    end_p = min(doc_len, start_p + chunk_sz, start_p + max_p)
    default_sub = infer_subject_from_filename(file_target)
    print(f"Processing pages {start_p + 1} to {end_p} (Total in doc: {doc_len}, Inferred Subject: {default_sub})...")

    total_extracted = 0

    for page_idx in range(start_p, end_p):
        page_num = page_idx + 1
        page = doc.load_page(page_idx)
        pix = page.get_pixmap(dpi=200)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        page_bytes = buf.getvalue()

        prompt = f"""
        EXAM PROFILE TARGET CONTEXT:
        Competitive Indian Engineering & Medical Entrance Examination (JEE / NEET).
        Default Subject: {default_sub}.

        CRITICAL ASSIGNMENT REQUIREMENT:
        Mentally scan the entire page. Extract every explicit question item block present on this document page layout.
        For each question, extract:
        - "subject": "Physics" | "Chemistry" | "Biology" | "Mathematics"
        - "chapter_name": NCERT Chapter name (e.g. "Kinematics", "Thermodynamics", "Chemical Bonding", "Genetics")
        - "topic_name": Specific subtopic (e.g. "Projectile Motion", "Hybridization")
        - "problem_type": Problem classification (e.g. "Numerical", "Assertion-Reason", "Match Column", "Conceptual Application")
        - "question_text": Full question text with equations in clean text/LaTeX
        - "options": list of option strings like ["(A) ...", "(B) ...", "(C) ...", "(D) ..."]
        - "correct_answer": Correct option letter if visible, else null
        - "difficulty_score": Float 1.0 to 10.0 (1 = trivial formula recall, 10 = multi-concept Olympiad level)
        - "lengthy_score": Float 1.0 to 10.0
        - "multi_concept_score": Float 1.0 to 10.0
        - "bounding_box": [ymin, xmin, ymax, xmax] scaled 0 to 1000
        """

        part = types.Part.from_bytes(data=page_bytes, mime_type="image/jpeg")

        questions = []
        for attempt in range(4):
            try:
                active_client = get_gemini_client() or client
                current_model = GEMINI_MODEL if attempt < 2 else GEMINI_FALLBACK_MODEL
                resp = active_client.models.generate_content(
                    model=current_model,
                    contents=[part, prompt],
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.0
                    )
                )
                raw_text = (resp.text or "").strip()
                if raw_text.startswith("```json"):
                    raw_text = raw_text[7:]
                if raw_text.startswith("```"):
                    raw_text = raw_text[3:]
                if raw_text.endswith("```"):
                    raw_text = raw_text[:-3]
                raw_text = raw_text.strip()
                if raw_text:
                    data = json.loads(raw_text)
                    questions = data if isinstance(data, list) else data.get("questions", [])
                break
            except Exception as e:
                err_str = str(e)
                print(f"Page {page_num} attempt {attempt+1} ({current_model}) warning: {err_str[:120]}")
                # Backoff with jitter on rate limits
                sleep_sec = (attempt + 1) * 3 + random.uniform(1.0, 3.0)
                time.sleep(sleep_sec)

        print(f"[{time.strftime('%X')}] Page {page_num}: Extracted {len(questions)} questions from Gemini. Processing items...")

        def process_q_task(item):
            q_idx, q = item
            try:
                bbox = q.get("bounding_box", [0, 0, 1000, 1000])
                cropped = crop_bbox(img, bbox)
                cbuf = io.BytesIO()
                cropped.save(cbuf, format="PNG")

                qid = f"batch_{args.job_id}_p{page_num}_q{q_idx+1}"
                b2_url = upload_question_image_b2(cbuf.getvalue(), args.job_id, qid)

                sub = q.get("subject") or default_sub
                ch = q.get("chapter_name") or q.get("chapter") or "General Chapter"
                top = q.get("topic_name") or q.get("topic") or ch
                prob = q.get("problem_type") or "Conceptual Application"
                worker_client = get_gemini_client() or active_client
                tax_ids = evolve_taxonomy_path(sub, ch, top, prob, "11", worker_client)

                q_text = q.get("question_text", f"Question {q_idx+1}")
                embedding = generate_embedding(q_text, worker_client)

                execute_query(
                    """
                    INSERT OR REPLACE INTO questions_bank (
                        id, project_id, question_id, image_url, question_text, options_json,
                        difficulty_score, lengthy_score, multi_concept_score,
                        embedding_json, subject_id, chapter_id, topic_id, problem_type_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        qid, args.job_id, q.get("question_id", qid), b2_url, q_text,
                        json.dumps(q.get("options", [])),
                        float(q.get("difficulty_score", 5.0)),
                        float(q.get("lengthy_score", 5.0)),
                        float(q.get("multi_concept_score", 1.0)),
                        json.dumps(embedding),
                        tax_ids["subject_id"], tax_ids["chapter_id"],
                        tax_ids["topic_id"], tax_ids["problem_type_id"]
                    )
                )
                return True, q_idx + 1, tax_ids
            except Exception as q_err:
                print(f"Error processing question {q_idx+1} on page {page_num}: {q_err}")
                return False, q_idx + 1, None

        if questions:
            max_th = min(len(questions), 4) # Concurrency of 4 avoids hitting Gemini per-minute RPM limits
            with ThreadPoolExecutor(max_workers=max_th) as executor:
                futures = [executor.submit(process_q_task, (idx, q)) for idx, q in enumerate(questions)]
                for fut in as_completed(futures):
                    ok, q_num, tax_res = fut.result()
                    if ok:
                        total_extracted += 1

        if args.callback_url:
            post_callback(args.callback_url, args.callback_secret, {
                "job_id": args.job_id,
                "processed_pages": page_num,
                "total_pages": end_p,
                "extracted_questions_count": total_extracted,
                "status": "completed" if page_num == end_p else "processing"
            })

        print(f"Page {page_num}/{end_p} complete. Extracted so far: {total_extracted} questions.")

    print(f"=== Chunk Complete: Pages {start_p + 1} to {end_p}. Total Questions in chunk: {total_extracted} ===")

    if end_p >= doc_len and str(args.delete_on_complete).lower() in ("true", "1", "yes"):
        print(f"=== Document Fully Completed! All {doc_len} pages processed. ===")
        if os.path.exists(file_target):
            delete_processed_pdf(file_target, args.branch)

    return total_extracted
