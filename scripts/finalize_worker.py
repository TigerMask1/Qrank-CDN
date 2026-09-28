"""
Runs inside GitHub Actions (see .github/workflows/finalize_paper.yml).

Reads pending_jobs/{project_id}/manifest.json (pushed by the Render backend
right after finalize) plus the page images that were already pushed to
processing_pages/{project_id}/{page}.jpg progressively throughout the review
session (see main.py's _push_page_image_bg). No PDF, no PyMuPDF — this is
pure Pillow cropping of images that already exist in the repo, which is
what let the Render backend drop the pymupdf dependency from this job
entirely and is why this workflow no longer needs the source PDF at all.

Writes the cropped/stitched results to questions_db/{project_id}/ and
POSTs the result back to the Render app so it can fill in the image_url on
the questions_bank rows that were already created (with the difficulty
scores) back when /finalize responded to the student.
"""
import os
import io
import sys
import json
import requests
import shutil
import subprocess
from PIL import Image

CALLBACK_URL     = os.environ.get("CALLBACK_URL", "").rstrip("/")
CALLBACK_SECRET  = os.environ.get("CALLBACK_SECRET", "")
GITHUB_REPO      = os.environ.get("GITHUB_REPOSITORY", "")
GITHUB_BRANCH    = os.environ.get("GITHUB_REF_NAME", "main")

def crop_bbox_from_page(project_id, page_num, bbox_norm, pad_px=10):
    page_path = os.path.join("processing_pages", project_id, f"{page_num}.jpg")
    if not os.path.exists(page_path):
        raise FileNotFoundError(f"Missing page image: {page_path}")
    img = Image.open(page_path).convert("RGB")
    w, h = img.size
    ymin, xmin, ymax, xmax = bbox_norm
    x0 = max(0, (xmin / 1000.0) * w - pad_px)
    y0 = max(0, (ymin / 1000.0) * h - pad_px)
    x1 = min(w, (xmax / 1000.0) * w + pad_px)
    y1 = min(h, (ymax / 1000.0) * h + pad_px)
    return img.crop((x0, y0, x1, y1))

def stitch_vertical(images):
    width  = max(im.width for im in images)
    height = sum(im.height for im in images)
    canvas = Image.new("RGB", (width, height), "white")
    y = 0
    for im in images:
        canvas.paste(im, (0, y))
        y += im.height
    return canvas

def post_callback(project_id, payload: dict):
    if not CALLBACK_URL or not CALLBACK_SECRET:
        print("No CALLBACK_URL/CALLBACK_SECRET set — printing result instead:")
        print(json.dumps(payload, indent=2))
        return
    payload["project_id"] = project_id
    try:
        resp = requests.post(
            f"{CALLBACK_URL}/api/review/{project_id}/finalize-callback",
            json=payload,
            headers={"X-Finalize-Secret": CALLBACK_SECRET},
            timeout=30
        )
        print(f"Callback response for {project_id}: {resp.status_code} {resp.text[:300]}")
    except Exception as e:
        print(f"Callback failed for {project_id}: {e}")

def process_project(project_id):
    print(f"Processing project: {project_id}")
    job_dir = os.path.join("pending_jobs", project_id)
    manifest_path = os.path.join(job_dir, "manifest.json")
    out_dir = os.path.join("questions_db", project_id)
    
    if not os.path.exists(manifest_path):
        print(f"Missing manifest for {project_id} — skipping.")
        return False
        
    with open(manifest_path) as f:
        manifest = json.load(f)

    os.makedirs(out_dir, exist_ok=True)
    results = []

    for group in manifest.get("groups", []):
        members = sorted(group["members"], key=lambda m: m["page"])
        try:
            crops = [crop_bbox_from_page(project_id, m["page"], m["bbox"]) for m in members if m.get("bbox")]
            if not crops:
                continue
            final_image = crops[0] if len(crops) == 1 else stitch_vertical(crops)
        except Exception as e:
            print(f"Crop failed for group {group.get('key')}: {e}")
            results.append({"question_id": group["question_id"], "status": "failed", "error": str(e)})
            continue

        question_id = group["question_id"]
        safe_name = question_id.replace(" ", "_").replace("*", "")
        file_name = f"{safe_name}_{group['key'][:8]}.png"
        out_path = os.path.join(out_dir, file_name)
        final_image.save(out_path, format="PNG")

        image_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{out_dir}/{file_name}"
        results.append({
            "question_id":  question_id,
            "page_number":  members[0]["page"],
            "is_continued": len(crops) > 1,
            "image_url":    image_url,
            "status":       "ok"
        })

    post_callback(project_id, {"status": "done", "items": results})
    
    # Write to a file so the shell script knows which ones to git rm
    with open("processed_projects.txt", "a") as f:
        f.write(f"{project_id}\n")
    return True

def main():
    if not os.path.exists("pending_jobs"):
        print("No pending_jobs directory found.")
        return
        
    projects = [d for d in os.listdir("pending_jobs") if os.path.isdir(os.path.join("pending_jobs", d))]
    if not projects:
        print("No pending projects to process.")
        return
        
    open("processed_projects.txt", "w").close() # Clear file
    
    for pid in projects:
        process_project(pid)

if __name__ == "__main__":
    main()
