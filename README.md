# photo_dedup_ai

Find and review duplicate / near-duplicate photos and videos on macOS with a simple UI (Gradio).
Phase 1 focuses on **read-only review** with robust similarity for **JPEG, HEIC, RAW** and **MOV** (audio+visual).

## Goals (Phase 1)

- Scan multiple folders and index ~100K media files
- Exact duplicates (BLAKE3/SHA-256)
- Near-duplicates (pHash/dHash, ORB/SSIM)
- Video similarity via keyframes + audio MFCC
- Gradio UI to review clusters and export a plan (no file changes)

## Quickstart (dev)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
# (Phase 1 deps will be added later)
```
