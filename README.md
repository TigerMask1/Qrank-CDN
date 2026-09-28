# Qrank-CDN

Public CDN repository for [Qrank](https://github.com/TigerMask1/Qrank).

This repository serves as:
1. **GitHub Actions runner** — Heavy PDF cropping/stitching runs here (free public minutes, faster queues)
2. **Image CDN** — All question images, anchor calibration images served via `raw.githubusercontent.com`

## Directory structure

```
questions_db/       — Cropped question images per project (created by Actions)
static/anchors/     — Calibration anchor images (NEET 2019/2020)
processing_pages/   — Temporary page renders (cleaned up after finalization)
pending_jobs/       — Finalization manifests (queued by backend, processed by Actions)
scripts/            — Worker scripts run by GitHub Actions
.github/workflows/  — GitHub Actions workflows
```

## How it works

1. Student clicks **Finalize** → Render backend writes a manifest to `pending_jobs/{id}/manifest.json` in this repo via the GitHub API
2. Backend triggers `finalize_paper.yml` workflow on this repo
3. The Action processes ALL pending jobs in one run (buffer system)
4. Cropped images are committed to `questions_db/{id}/` — publicly accessible instantly
5. Worker POSTs the public image URLs back to the Render backend callback
6. Images are served directly from `raw.githubusercontent.com` — no auth needed
