"""
AIRstudy / QRank — Parallel & Autonomous Book Queue Coordinator.
Permits multiple runners to execute simultaneously without colliding:
- Runner 1 claims Pages 0-150.
- Runner 2 (triggered at the same time or while Runner 1 is on page 12) immediately claims Pages 151-300!
- Minimal Turso DB reads (1-2 rows per 45-minute chunk run, strictly conserving read quota).
- Once a book is 100% complete, automatically removes it from git and updates catalog.
"""
import os
import sys
import time
import json
import re
import requests

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import pymupdf as fitz
from turso_db import execute_query, init_turso_schema
from taxonomy_engine import warm_taxonomy_cache
from scripts.batch_pdf_worker import (
    process_single_pdf,
    resolve_lfs_pointer_if_needed,
    delete_processed_pdf,
    auto_delete_book_from_git
)

class QueueArgs:
    def __init__(self, start_page=0, chunk_size=150, total_pages=500, branch="feature/air-study-qmatch-upgrade", delete_on_complete=False):
        self.start_page = start_page
        self.chunk_size = chunk_size
        self.total_pages = total_pages
        self.branch = branch
        self.delete_on_complete = delete_on_complete
        self.job_id = None
        self.pdf_url = None
        self.callback_url = os.getenv("CALLBACK_URL", "")
        self.callback_secret = os.getenv("CALLBACK_SECRET", "")
        self.auto_chain = False

def init_claim_table():
    """Initializes the concurrency claims table in Turso DB."""
    schema = """
    CREATE TABLE IF NOT EXISTS book_chunk_claims (
        id TEXT PRIMARY KEY,
        file_name TEXT NOT NULL,
        start_page INTEGER NOT NULL,
        end_page INTEGER NOT NULL,
        total_pages INTEGER NOT NULL,
        status TEXT DEFAULT 'claimed',
        runner_id TEXT,
        extracted_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(file_name, start_page)
    );
    CREATE INDEX IF NOT EXISTS idx_chunk_claims_file ON book_chunk_claims(file_name, start_page);
    """
    for stmt in schema.split(";"):
        s = stmt.strip()
        if s:
            try:
                execute_query(s)
            except Exception:
                pass

def get_pending_books():
    """Scans incoming_books/ and root directory for pending PDF books (Zero DB reads)."""
    pending = []
    queue_dir = "incoming_books"
    if os.path.isdir(queue_dir):
        for f in sorted(os.listdir(queue_dir)):
            if f.lower().endswith(".pdf"):
                full_p = os.path.normpath(os.path.join(queue_dir, f))
                pending.append(full_p)

    for f in sorted(os.listdir(".")):
        if f.lower().endswith(".pdf") and os.path.isfile(f):
            norm = os.path.normpath(f)
            if "paper_pdfs" not in norm and norm not in pending:
                pending.append(norm)

    return pending

def update_catalog_on_completion(book_name: str, total_pages: int):
    """Appends completion record to PROCESSED_BOOKS_CATALOG.md if not already recorded."""
    catalog_path = "PROCESSED_BOOKS_CATALOG.md"
    if not os.path.exists(catalog_path):
        return
    try:
        with open(catalog_path, "r", encoding="utf-8") as f:
            content = f.read()
        if book_name in content and "100% Ingested" in content:
            return
        entry = f"\n| - | **{book_name}** | Auto-Detected | {total_pages} pages | ✅ 100% Ingested | Ingested via Parallel Queue Pipeline |"
        with open(catalog_path, "a", encoding="utf-8") as f:
            f.write(entry)
        print(f"[Catalog] Updated {catalog_path} with completion of {book_name}")
    except Exception as e:
        print(f"[Catalog Error]: {e}")

def trigger_next_queue_cycle(branch: str):
    """Dispatches the next run of the queue workflow on GitHub Actions."""
    token = os.getenv("GITHUB_TOKEN") or os.getenv("PRIVATE_REPO_PAT")
    cdn_repo = os.getenv("GITHUB_CDN_REPO", "TigerMask1/Qrank-CDN")
    if not token:
        print("[Queue Warning] GITHUB_TOKEN not available; cannot trigger next queue cycle.")
        return False

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

    url = f"https://api.github.com/repos/{cdn_repo}/actions/workflows/auto_book_queue.yml/dispatches"
    payload = {
        "ref": "main",
        "inputs": {
            "branch": branch
        }
    }
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=20)
        if res.status_code in [200, 204]:
            print(f"[Queue] Successfully dispatched next cycle on {cdn_repo}")
            return True
        else:
            print(f"[Queue Dispatch Note]: Response {res.status_code}")
    except Exception as e:
        print(f"[Queue Dispatch Error]: {e}")

    return False

def claim_next_chunk(pending_books, chunk_size=150, runner_id=None):
    """
    Finds the next available slice across pending books and atomically claims it.
    If Runner 1 claimed pages 0-150, Runner 2 immediately claims pages 151-300.
    Consumes minimal Turso DB read rows (1-2 rows per runner).
    """
    init_claim_table()

    for book_path in pending_books:
        file_name = os.path.basename(book_path)
        resolve_lfs_pointer_if_needed(book_path)

        # 1. Total pages locally via PyMuPDF (0 DB reads!)
        try:
            doc = fitz.open(book_path)
            total_pages = len(doc)
            doc.close()
        except Exception as e:
            print(f"[Queue] Skipping unreadable {file_name}: {e}")
            continue

        if total_pages <= 0:
            continue

        # 2. Check maximum claimed page for this book (1 single row read!)
        rows = execute_query(
            "SELECT MAX(end_page) as max_claimed FROM book_chunk_claims WHERE file_name = ?",
            (file_name,)
        )
        max_claimed = 0
        if rows and rows[0].get("max_claimed") is not None:
            max_claimed = int(rows[0]["max_claimed"])

        # 3. Check for any stale claims (> 90 minutes / 5400s without completion) (1 single row read)
        stale_rows = execute_query(
            """
            SELECT id, start_page, end_page FROM book_chunk_claims 
            WHERE file_name = ? AND status = 'claimed' 
            AND (strftime('%s', 'now') - strftime('%s', updated_at)) > 5400
            LIMIT 1
            """,
            (file_name,)
        )
        if stale_rows:
            stale_id = stale_rows[0]["id"]
            stale_start = int(stale_rows[0]["start_page"])
            stale_end = int(stale_rows[0]["end_page"])
            print(f"[Queue] Reclaiming stale chunk {stale_start}-{stale_end} for {file_name}...")
            execute_query(
                "UPDATE book_chunk_claims SET status = 'claimed', runner_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (runner_id, stale_id)
            )
            return book_path, stale_start, stale_end, total_pages, stale_id

        # 4. If this book has unclaimed pages:
        if max_claimed < total_pages:
            next_start = max_claimed
            next_end = min(total_pages, next_start + chunk_size)
            claim_id = f"{file_name}_p{next_start}_{next_end}"

            try:
                execute_query(
                    """
                    INSERT INTO book_chunk_claims (id, file_name, start_page, end_page, total_pages, status, runner_id)
                    VALUES (?, ?, ?, ?, ?, 'claimed', ?)
                    """,
                    (claim_id, file_name, next_start, next_end, total_pages, runner_id)
                )
                print(f"🎯 [Parallel Claim SUCCESS] Claimed pages {next_start + 1} to {next_end} of '{file_name}'!")
                return book_path, next_start, next_end, total_pages, claim_id
            except Exception as coll_err:
                # Race condition: Another parallel runner claimed at the identical millisecond
                print(f"[Claim Race Detected] Another runner claimed slice simultaneously: {coll_err}. Trying next...")
                continue

        # 5. If this book is already fully claimed (max_claimed >= total_pages):
        # Check if all chunks have finished processing (1 single row read)
        comp_rows = execute_query(
            "SELECT COUNT(*) as pending_cnt FROM book_chunk_claims WHERE file_name = ? AND status != 'completed'",
            (file_name,)
        )
        pending_cnt = int(comp_rows[0]["pending_cnt"]) if comp_rows else 0
        if pending_cnt == 0:
            print(f"🎉 [Book Complete] All chunks of '{file_name}' are 100% completed! Cleaning up...")
            auto_delete_book_from_git(book_path, branch=os.getenv("GITHUB_BRANCH", "feature/air-study-qmatch-upgrade"))
            update_catalog_on_completion(file_name, total_pages)
            continue
        else:
            print(f"⏳ [Book In-Flight] '{file_name}' is 100% claimed across parallel runners ({pending_cnt} chunks processing). Checking next book...")
            continue

    return None, 0, 0, 0, None

def run_queue_pipeline():
    print("===================================================================")
    print("===      AIRstudy / QRank Parallel Book Queue Pipeline          ===")
    print("===================================================================")

    init_turso_schema()
    warm_taxonomy_cache()

    pending_books = get_pending_books()
    if not pending_books:
        print("🎉 [AIRstudy Queue] No pending books found in 'incoming_books/' or root.")
        return

    print(f"[AIRstudy Queue] Found {len(pending_books)} books in queue.")
    runner_id = f"run_{os.getenv('GITHUB_RUN_ID', str(int(time.time())))}"

    book_path, start_p, end_p, total_pages, claim_id = claim_next_chunk(
        pending_books,
        chunk_size=150,
        runner_id=runner_id
    )

    if not book_path:
        print("🎉 [AIRstudy Queue] All pending books are fully claimed or completed by active runners!")
        return

    file_name = os.path.basename(book_path)
    base_name = os.path.splitext(file_name)[0]
    clean_slug = re.sub(r'[^a-zA-Z0-9]', '_', base_name).strip('_').lower()[:32]
    job_id = f"book_{clean_slug}_p{start_p}_{end_p}"

    args = QueueArgs(
        start_page=start_p,
        chunk_size=(end_p - start_p),
        total_pages=total_pages,
        branch=os.getenv("GITHUB_BRANCH", "feature/air-study-qmatch-upgrade"),
        delete_on_complete=False
    )
    args.job_id = job_id
    args.pdf_url = book_path

    print(f"\n🚀 [Runner {runner_id}] Processing: '{file_name}' (Pages {start_p + 1} to {end_p} of {total_pages})")

    extracted = process_single_pdf(book_path, args)

    # 1. Mark this chunk as completed
    execute_query(
        "UPDATE book_chunk_claims SET status = 'completed', extracted_count = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (extracted, claim_id)
    )

    # 2. Record in batch_pdf_jobs for cataloging and backward compatibility
    try:
        execute_query(
            """
            INSERT INTO batch_pdf_jobs (
                job_id, file_name, file_url, total_pages, processed_pages, extracted_questions_count, status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'completed', CURRENT_TIMESTAMP)
            ON CONFLICT(job_id) DO UPDATE SET
                processed_pages = excluded.processed_pages,
                extracted_questions_count = excluded.extracted_questions_count,
                status = excluded.status,
                updated_at = CURRENT_TIMESTAMP
            """,
            (job_id, file_name, book_path, total_pages, end_p, extracted)
        )
    except Exception as db_e:
        print(f"[Turso Jobs Update Note]: {db_e}")

    print(f"\n✅ [Runner {runner_id}] Finished Chunk {start_p + 1}-{end_p}. Extracted {extracted} questions.")

    # 3. Check if all chunks for this book are completed
    comp_rows = execute_query(
        "SELECT COUNT(*) as pending_cnt FROM book_chunk_claims WHERE file_name = ? AND status != 'completed'",
        (file_name,)
    )
    pending_cnt = int(comp_rows[0]["pending_cnt"]) if comp_rows else 0
    if pending_cnt == 0:
        # Check if entire page range covered
        max_rows = execute_query("SELECT MAX(end_page) as max_e FROM book_chunk_claims WHERE file_name = ?", (file_name,))
        max_e = int(max_rows[0]["max_e"]) if max_rows and max_rows[0].get("max_e") else 0
        if max_e >= total_pages and total_pages > 0:
            print(f"🎉 [Book Complete] All chunks of '{file_name}' ({total_pages}/{total_pages} pages) are 100% finished!")
            auto_delete_book_from_git(book_path, branch=args.branch)
            update_catalog_on_completion(file_name, total_pages)

    # 4. Trigger next cycle to keep the pipeline moving if books remain
    remaining = get_pending_books()
    if remaining:
        time.sleep(10)
        trigger_next_queue_cycle(branch=args.branch)

if __name__ == "__main__":
    run_queue_pipeline()
