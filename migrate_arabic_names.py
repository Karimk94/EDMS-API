"""
migrate_arabic_names.py
========================
One-time migration script to normalise Arabic face names stored before the
fix that ensures names are always saved in English.

What it fixes
-------------
1. **Oracle PROFILE.ABSTRACT** – Any document whose ABSTRACT contains a
   "VIPs: ..." section with Arabic names has those names replaced with their
   English equivalents from LKP_PERSON (or via the Translator API as a
   fallback).

2. **ChromaDB** (Face Recognition py) – Any face embedding whose "name"
   metadata is non-ASCII (Arabic) is translated to English and updated
   in-place.

Usage
-----
Run from the EDMS API project root so that .env is picked up:

    cd "d:\\projects\\new EDMS py\\EDMS API"
    python migrate_arabic_names.py

Optional flags
    --dry-run          Print what would change, do NOT write anything.
    --chroma-path PATH Path to the ChromaDB folder
                       (default: ..\Face Recognition py\chroma_db)
    --translator-url   Override TRANSLATOR_REPHRASER_API_URL from .env
"""

import argparse
import logging
import os
import re
import sys

import oracledb
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("migrate")

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()

DB_HOST        = os.getenv("DB_HOST")
DB_PORT        = os.getenv("DB_PORT")
DB_SERVICE     = os.getenv("DB_SERVICE_NAME")
DB_USER        = os.getenv("DB_USERNAME")
DB_PASS        = os.getenv("DB_PASSWORD")
TRANSLATOR_URL = os.getenv("TRANSLATOR_REPHRASER_API_URL", "").rstrip("/")

DEFAULT_CHROMA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "Face Recognition py", "chroma_db"
)

# ---------------------------------------------------------------------------
# Translator helper
# ---------------------------------------------------------------------------

def translate_to_english(text: str) -> str | None:
    """Call the Translator/Rephraser API and return the English translation."""
    if not TRANSLATOR_URL:
        log.warning("TRANSLATOR_REPHRASER_API_URL not set — cannot translate via API.")
        return None
    try:
        resp = requests.post(
            f"{TRANSLATOR_URL}/generate",
            json={"text": text, "task": "translate"},
            timeout=60,
            stream=True,
        )
        resp.raise_for_status()
        result = ""
        for line in resp.iter_lines():
            if line:
                decoded = line.decode("utf-8")
                if decoded.startswith("data: "):
                    chunk = decoded[6:]
                    if chunk.strip() == "[END_OF_STREAM]":
                        break
                    result += chunk
        return result.strip() or None
    except Exception as exc:
        log.error("Translation failed for '%s': %s", text, exc)
        return None


# ---------------------------------------------------------------------------
# Oracle helpers
# ---------------------------------------------------------------------------

def get_oracle_connection():
    dsn = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"
    return oracledb.connect(user=DB_USER, password=DB_PASS, dsn=dsn)


def build_arabic_to_english_map(cursor) -> dict[str, str]:
    """
    Return a dict  { arabic_name -> english_name }  from LKP_PERSON
    for every row that has a non-null NAME_ARABIC.
    """
    cursor.execute(
        "SELECT NAME_ARABIC, NAME_ENGLISH FROM LKP_PERSON "
        "WHERE NAME_ARABIC IS NOT NULL AND NAME_ENGLISH IS NOT NULL"
    )
    return {row[0].strip(): row[1].strip() for row in cursor.fetchall()}


def is_arabic(text: str) -> bool:
    return not text.isascii()


def resolve_name(name: str, ar_to_en: dict[str, str]) -> str:
    """
    Return the English equivalent of *name*.
    Priority: LKP_PERSON lookup → Translator API → original value.
    """
    name = name.strip()
    if not is_arabic(name):
        return name  # already English

    # 1. Direct LKP_PERSON lookup
    if name in ar_to_en:
        return ar_to_en[name]

    # 2. Translator API
    translated = translate_to_english(name)
    if translated:
        return translated

    # 3. Fallback
    log.warning("  Could not resolve Arabic name '%s' — keeping as-is.", name)
    return name


def fix_abstract(abstract: str, ar_to_en: dict[str, str]) -> tuple[str, bool]:
    """
    Replace Arabic VIP names inside a PROFILE.ABSTRACT with their English
    equivalents.  Returns (new_abstract, changed).
    """
    match = re.search(r"(VIPs\s*:\s*)(.*)", abstract, re.IGNORECASE)
    if not match:
        return abstract, False

    prefix = match.group(1)
    names_str = match.group(2)
    names = [n.strip() for n in names_str.split(",") if n.strip()]

    resolved = [resolve_name(n, ar_to_en) for n in names]

    if resolved == names:
        return abstract, False  # nothing changed

    new_vips = prefix + ", ".join(sorted(set(resolved)))
    new_abstract = abstract[: match.start()] + new_vips + abstract[match.end() :]
    return new_abstract, True


def migrate_oracle(dry_run: bool):
    log.info("=" * 60)
    log.info("PHASE 1 — Oracle PROFILE.ABSTRACT")
    log.info("=" * 60)

    conn = get_oracle_connection()
    cursor = conn.cursor()

    log.info("Loading Arabic→English map from LKP_PERSON …")
    ar_to_en = build_arabic_to_english_map(cursor)
    log.info("  %d entries loaded.", len(ar_to_en))

    # Fetch all documents that have a VIPs section
    cursor.execute(
        "SELECT DOCNUMBER, ABSTRACT FROM PROFILE "
        "WHERE UPPER(ABSTRACT) LIKE '%VIPS:%' AND ABSTRACT IS NOT NULL"
    )
    rows = cursor.fetchall()
    log.info("Found %d document(s) with a VIPs section.", len(rows))

    updated = 0
    skipped = 0

    for docnumber, abstract in rows:
        new_abstract, changed = fix_abstract(abstract, ar_to_en)
        if not changed:
            skipped += 1
            continue

        log.info("  [DOC %s] VIPs section updated:", docnumber)
        # Show diff of just the VIPs line
        old_vips = re.search(r"VIPs\s*:.*", abstract, re.IGNORECASE)
        new_vips = re.search(r"VIPs\s*:.*", new_abstract, re.IGNORECASE)
        log.info("    OLD: %s", old_vips.group(0) if old_vips else "?")
        log.info("    NEW: %s", new_vips.group(0) if new_vips else "?")

        if not dry_run:
            cursor.execute(
                "UPDATE PROFILE SET ABSTRACT = :1 WHERE DOCNUMBER = :2",
                [new_abstract, docnumber],
            )
        updated += 1

    if not dry_run and updated:
        conn.commit()

    log.info(
        "Oracle: %d document(s) %s, %d already clean.",
        updated,
        "would be updated (dry-run)" if dry_run else "updated",
        skipped,
    )
    cursor.close()
    conn.close()


# ---------------------------------------------------------------------------
# ChromaDB helpers
# ---------------------------------------------------------------------------

def migrate_chroma(chroma_path: str, dry_run: bool):
    log.info("=" * 60)
    log.info("PHASE 2 — ChromaDB face embeddings")
    log.info("=" * 60)

    try:
        import chromadb
    except ImportError:
        log.error("chromadb package not found. Install it or run this script from the Face Recognition venv.")
        return

    chroma_path = os.path.abspath(chroma_path)
    if not os.path.isdir(chroma_path):
        log.warning("ChromaDB path does not exist: %s — skipping.", chroma_path)
        return

    log.info("Connecting to ChromaDB at: %s", chroma_path)
    client = chromadb.PersistentClient(path=chroma_path)

    try:
        collection = client.get_collection("faces")
    except Exception as exc:
        log.error("Could not open 'faces' collection: %s", exc)
        return

    all_entries = collection.get(include=["metadatas", "embeddings"])
    ids       = all_entries.get("ids", [])
    metadatas = all_entries.get("metadatas", [])
    embeddings= all_entries.get("embeddings", [])

    log.info("Total face entries in ChromaDB: %d", len(ids))

    # Build Arabic→English map from Oracle for cross-reference
    try:
        conn = get_oracle_connection()
        cur  = conn.cursor()
        cur.execute(
            "SELECT NAME_ARABIC, NAME_ENGLISH FROM LKP_PERSON "
            "WHERE NAME_ARABIC IS NOT NULL AND NAME_ENGLISH IS NOT NULL"
        )
        # ChromaDB stores names lowercased with underscores, so normalise the
        # lookup key the same way.
        ar_to_en_chroma = {
            row[0].strip().replace(" ", "_").lower(): row[1].strip()
            for row in cur.fetchall()
        }
        cur.close()
        conn.close()
    except Exception as exc:
        log.warning("Could not load LKP_PERSON for ChromaDB migration: %s — will use translator only.", exc)
        ar_to_en_chroma = {}

    to_update_ids        = []
    to_update_metadatas  = []
    to_update_embeddings = []

    for entry_id, meta, emb in zip(ids, metadatas, embeddings):
        stored_name = meta.get("name", "")
        if stored_name.isascii():
            continue  # already English

        log.info("  [%s] Arabic name detected: '%s'", entry_id, stored_name)

        # Try LKP_PERSON first (name is already underscore+lower in ChromaDB)
        english_name = ar_to_en_chroma.get(stored_name)

        if english_name:
            english_key = english_name.replace(" ", "_").lower()
            log.info("    → resolved via LKP_PERSON: '%s'", english_key)
        else:
            # Try translator (send the original Arabic with spaces restored)
            arabic_display = stored_name.replace("_", " ")
            translated = translate_to_english(arabic_display)
            if translated:
                english_key = translated.strip().replace(" ", "_").lower()
                log.info("    → translated via API: '%s'", english_key)
            else:
                log.warning("    → could not resolve '%s' — skipping.", stored_name)
                continue

        to_update_ids.append(entry_id)
        to_update_metadatas.append({"name": english_key})
        to_update_embeddings.append(emb)

    log.info(
        "ChromaDB: %d entry/entries %s.",
        len(to_update_ids),
        "would be updated (dry-run)" if dry_run else "to update",
    )

    if not dry_run and to_update_ids:
        # ChromaDB update() updates metadata (and optionally embeddings) for
        # existing IDs.  We pass embeddings back unchanged.
        collection.update(
            ids=to_update_ids,
            metadatas=to_update_metadatas,
            embeddings=to_update_embeddings,
        )
        log.info("ChromaDB: %d entry/entries updated successfully.", len(to_update_ids))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Migrate Arabic VIP names to English in Oracle DB and ChromaDB."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing anything.",
    )
    parser.add_argument(
        "--chroma-path",
        default=DEFAULT_CHROMA_PATH,
        help=f"Path to the ChromaDB folder (default: {DEFAULT_CHROMA_PATH})",
    )
    parser.add_argument(
        "--translator-url",
        default=None,
        help="Override TRANSLATOR_REPHRASER_API_URL from .env.",
    )
    parser.add_argument(
        "--skip-oracle",
        action="store_true",
        help="Skip the Oracle PROFILE.ABSTRACT migration.",
    )
    parser.add_argument(
        "--skip-chroma",
        action="store_true",
        help="Skip the ChromaDB migration.",
    )
    args = parser.parse_args()

    if args.translator_url:
        global TRANSLATOR_URL
        TRANSLATOR_URL = args.translator_url.rstrip("/")

    if args.dry_run:
        log.info("*** DRY-RUN MODE — no changes will be written ***")

    if not args.skip_oracle:
        try:
            migrate_oracle(dry_run=args.dry_run)
        except Exception as exc:
            log.error("Oracle migration failed: %s", exc, exc_info=True)
            sys.exit(1)

    if not args.skip_chroma:
        migrate_chroma(chroma_path=args.chroma_path, dry_run=args.dry_run)

    log.info("Migration complete.")


if __name__ == "__main__":
    main()
