# Smart EDMS — Backend API

**Smart EDMS API** is a Python-based middleware API built with **FastAPI** that serves as the central processing hub for the Smart EDMS ecosystem. It bridges the frontend application with an **Oracle Database**, an on-premise **SOAP/WSDL Document Management System (DMS)**, and a suite of **AI microservices** for document intelligence (image captioning, OCR, face recognition, video summarization, translation, and semantic search). It also manages user authentication, authorization, folder hierarchies, document sharing, tagging, watermarking, a background processing queue, EDMS user provisioning, eDOCS server cache management, and organizational hierarchy administration.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Environment Variables](#environment-variables)
- [Setup & Installation](#setup--installation)
- [Running the API](#running-the-api)
- [API Routes](#api-routes)
  - [Authentication & Users](#authentication--users)
  - [Documents](#documents)
  - [Media](#media)
  - [Folders](#folders)
  - [Tags & Persons](#tags--persons)
  - [Events](#events)
  - [Favorites](#favorites)
  - [Memories](#memories)
  - [Sharing](#sharing)
  - [Admin Panel](#admin-panel)
  - [eDOCS Admin](#edocs-admin)
  - [EMS Admin](#ems-admin)
  - [EDMS People Management](#edms-people-management)
  - [Profile Search](#profile-search)
- [AI Processing Pipeline](#ai-processing-pipeline)
- [Background Processing Queue](#background-processing-queue)
- [Worker Process](#worker-process)
- [Vector Search (ChromaDB)](#vector-search-chromadb)
- [WSDL / SOAP Client](#wsdl--soap-client)
- [Database Layer](#database-layer)
- [Utilities](#utilities)
- [Middleware Stack](#middleware-stack)
- [Security](#security)
- [Deployment](#deployment)
- [Batch Scripts & Tooling](#batch-scripts--tooling)
- [Blocklist & Content Filtering](#blocklist--content-filtering)

---

## Architecture Overview

```
┌─────────────────┐      ┌──────────────────────────────────────────┐
│                 │      │         Smart EDMS API (FastAPI)         │
│   Next.js       │◄────►│                                          │
│   Frontend      │ REST │  Routes ► Services ► Database Layer      │
│                 │      │    │          │            │              │
└─────────────────┘      │    │          │            ├─► Oracle DB  │
                         │    │          │            ├─► SQLite     │
                         │    │          │            └─► ChromaDB   │
                         │    │          │                           │
                         │    │          ├─► WSDL/SOAP DMS Server   │
                         │    │          ├─► Image Captioning API    │
                         │    │          ├─► OCR API                 │
                         │    │          ├─► Face Recognition API    │
                         │    │          ├─► Video Summarizer API    │
                         │    │          ├─► Translator/Rephraser API│
                         │    │          └─► Embedding API           │
                         └──────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│  worker.py  (Standalone Processing Worker)  │
│  ┌─────────────────────────────────────┐    │
│  │  processing_worker_loop()           │    │
│  │  Claims jobs from SQLite queue      │    │
│  │  Calls processor.py ► AI services   │    │
│  │  Updates Oracle + SQLite on result  │    │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
```

The API acts as a **middleware layer** — neither the frontend nor the AI services directly access the Oracle database or the DMS. All interactions flow through this API. The background AI processing runs in a **separate worker process** (`worker.py`) to keep the FastAPI request handling isolated from heavy queue work.

---

## Technology Stack

| Component | Technology |
|---|---|
| **Framework** | FastAPI (async, ASGI) |
| **ASGI Server** | Uvicorn (multi-worker support) |
| **Database** | Oracle DB (via `oracledb` thin driver) |
| **DMS Integration** | SOAP/WSDL via `zeep` (with custom binary buffer parser) |
| **Vector Store** | ChromaDB (persistent, local) |
| **Local Queue DB** | SQLite (WAL mode, busy timeout) |
| **Image Processing** | Pillow, PyMuPDF (fitz) |
| **Video Processing** | MoviePy, FFmpeg (hardware-accelerated: NVIDIA/AMD/Intel QSV) |
| **Validation** | Pydantic v2, Marshmallow |
| **Rate Limiting** | SlowAPI |
| **Session Management** | Starlette SessionMiddleware (cookie-based, 60-day TTL) |
| **Analytics** | PostHog |
| **Password Hashing** | MD5 Base64 (legacy DMS compatibility) |

---

## Project Structure

```
EDMS API/
├── app.py                    # FastAPI application entry point, middleware stack & lifespan setup
├── worker.py                 # Standalone background processing worker (separate process)
├── db_connector.py           # Central re-export hub for all database functions
├── api_client.py             # HTTP clients for external AI microservices (sync, stream-based)
├── vector_client.py          # ChromaDB vector database client & external embedding wrapper
├── backfill_vectors.py       # Standalone script to backfill vector embeddings for existing docs
│
├── routes/                   # API route handlers (organized by domain)
│   ├── auth.py               # Authentication, login/logout, user preferences, groups, trustees
│   ├── documents.py          # Document CRUD, upload, download, watermarked download, metadata
│   ├── media.py              # Image/PDF/video streaming, thumbnails, cache management
│   ├── folders.py            # Folder CRUD, move items, download folder as ZIP
│   ├── tags.py               # Tags, persons, shortlist, batch tag fetching
│   ├── events.py             # Event CRUD, document-event linking, journey data
│   ├── favorites.py          # Add/remove/list favorite documents
│   ├── memories.py           # "On This Day" memories feature
│   ├── sharing.py            # Shareable links, OTP verification, shared file streaming/download
│   ├── admin.py              # Admin panel: user management, processing queue control, tab permissions
│   ├── edocs_admin.py        # eDOCS server cache clearing (background task)
│   ├── ems_admin.py          # EMS organizational admin: agencies, departments, sections
│   ├── edms_people.py        # EDMS user provisioning: create/update/export users in PEOPLE table
│   └── profilesearch.py     # Advanced multi-criteria profile search w/ dynamic scoping
│
├── database/                 # Database access layer (Oracle, SQLite)
│   ├── __init__.py           # Package exports
│   ├── connection.py         # Oracle connection pool, async wrappers, performance indexes
│   ├── documents.py          # Document queries: fetch, filter, sort, process status, update
│   ├── media.py              # DMS media operations: login, get info, stream, thumbnails, cache
│   ├── folders.py            # Folder hierarchy queries, breadcrumbs, file listing
│   ├── tags.py               # Tag/keyword CRUD, batch operations, translations
│   ├── events.py             # Event CRUD, document-event joins, journey data aggregation
│   ├── favorites.py          # Favorites CRUD (per-user)
│   ├── memories.py           # "On This Day" query logic, journey data aggregation
│   ├── users.py              # User details, language/theme preference storage
│   ├── user_data.py          # EDMS user ID resolution, storage quota management
│   ├── groups.py             # DMS group queries (sync fallback)
│   ├── sharing.py            # Share link storage, OTP storage & verification, access logging
│   ├── admin.py              # Admin user management: add/delete/update EDMS users
│   ├── ems_admin.py          # EMS organizational data: agencies, departments, EMS sections
│   ├── edms_people.py        # EDMS user CRUD: PEOPLE table, PEOPLEGROUPS, NETWORK_ALIASES, HR linkage
│   ├── tab_permissions.py    # Per-user tab visibility permissions
│   ├── profilesearch.py     # Multi-criteria dynamic SQL builder for profile search
│   └── processing_queue.sqlite3  # SQLite database for local processing queue
│
├── schemas/                  # Pydantic/Marshmallow request/response schemas
│   ├── auth.py               # Login, user update, group, trustee, EDMS user CRUD schemas
│   ├── documents.py          # Document upload, metadata update, security schemas
│   ├── folders.py            # Folder create, rename, move schemas
│   ├── sharing.py            # Share link, OTP, folder contents, download schemas
│   ├── tags.py               # Tag, person, processing status schemas
│   ├── events.py             # Event creation schema
│   └── edocs_cache.py        # eDOCS cache clear request/response schemas
│
├── services/                 # Business logic & background processing
│   ├── processor.py          # AI document processing pipeline (caption, OCR, face, video)
│   ├── processing_queue.py   # SQLite-backed local processing queue, worker loop & mode control
│   └── edocs_cache.py        # OpenText eDOCS DM server cache clearing (passive, non-destructive)
│
├── utils/                    # Shared utilities
│   ├── common.py             # Auth helpers, MIME detection, email sending (OTP & share links),
│   │                         #   text cleaning, Content-Disposition builder, company logo loader
│   ├── watermark.py          # Image/PDF/video watermarking (FFmpeg HW-accelerated + MoviePy fallback)
│   ├── sanitize.py           # XSS prevention: input text & filename sanitization
│   ├── cache_eviction.py     # Periodic video cache cleanup (LRU eviction by file age)
│   ├── log_cleanup.py        # IIS/application log file cleanup (age-based deletion, 14-day default)
│   └── ttl_cache.py          # In-memory TTL cache for frequently-requested metadata
│
├── wsdl_client/              # SOAP/WSDL DMS integration layer
│   ├── __init__.py           # Package exports
│   ├── config.py             # WSDL URL and DMS credential configuration
│   ├── base.py               # Core SOAP client initialization & helpers
│   ├── auth.py               # DMS authentication (user login, system login with 30-min TTL cache)
│   ├── documents.py          # DMS document operations (upload, download, stream, delete, set security)
│   ├── folders.py            # DMS folder operations (create, rename, move, list, delete)
│   ├── users.py              # DMS user/group queries (groups for user, all groups, group members)
│   └── utils.py              # DMS binary buffer parsing (zlib decompression, UTF-16LE string extraction,
│                             #   user/group result normalization, folder content parsing)
│
├── chroma_db/                # ChromaDB persistent vector store data directory
├── thumbnail_cache/          # Persistent thumbnail image cache
├── temp_thumbnail_cache/     # Ephemeral thumbnail cache (24h TTL)
├── video_cache/              # Cached video files (LRU evicted every 6 hours)
├── download_cache/           # Cached file downloads for watermarking
├── static/                   # Static files served by FastAPI (e.g., company logo)
│   └── images/               # Logo and brand assets for email templates
├── blocklist.json            # Content filtering: stop words, profanity, meaningless characters
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (credentials, URLs, timeouts)
├── setup.bat                 # Windows setup script (venv + pip install)
├── run_api.bat               # Windows startup script (Uvicorn with 4 workers)
├── download_packages.bat     # Downloads pip wheels for offline/air-gapped deployment
├── install_packages.bat      # Installs packages from local wheel cache (offline)
├── create_archive.bat        # Creates a deployment archive ZIP (respects .gitignore)
├── web.config                # IIS deployment configuration (primary)
└── web2.config               # IIS deployment configuration (alternate)
```

---

## Environment Variables

The `.env` file configures all external connections, secrets, and tuning parameters. The following variables are supported:

### Oracle Database
| Variable | Description |
|---|---|
| `DB_HOST` | Oracle database server hostname/IP |
| `DB_PORT` | Oracle listener port |
| `DB_SERVICE_NAME` | Oracle service name |
| `DB_USERNAME` | Database user |
| `DB_PASSWORD` | Database password |

### SOAP DMS Server
| Variable | Description |
|---|---|
| `WSDL_URL` | WSDL endpoint for the DMS SOAP service |
| `DMS_USER` | System-level DMS username (for background operations) |
| `DMS_PASSWORD` | System-level DMS password |

### Email (SMTP)
| Variable | Description |
|---|---|
| `SMTP_SERVER` | SMTP mail server hostname |
| `SMTP_PORT` | SMTP port (typically 587) |
| `SMTP_USER` | SMTP authentication username |
| `SMTP_PASSWORD` | SMTP authentication password |
| `SMTP_SENDER_EMAIL` | Sender email address |
| `SMTP_SENDER_NAME` | Display name for sent emails |
| `COMPANY_NAME` | Company name shown in email templates |
| `SUPPORT_EMAIL` | Support email in email footer |
| `EMAIL_PRIMARY_COLOR` | Brand color for HTML email templates |
| `COMPANY_WEBSITE` | Company website URL in email footer |

### AI Microservice URLs
| Variable | Description |
|---|---|
| `CAPTIONING_API_URL` | Image captioning / object detection service |
| `OCR_API_URL` | Optical Character Recognition service (images & PDFs) |
| `FACE_API_URL` | Face recognition / identification service |
| `VIDEO_SUMMARIZER_API_URL` | Video analysis, transcription & summarization service |
| `TRANSLATOR_REPHRASER_API_URL` | Text translation (EN↔AR) & tokenization service |
| `EMBEDDING_API_URL` | Text embedding service (for semantic vector search) |

### Application
| Variable | Description | Default |
|---|---|---|
| `FLASK_SECRET_KEY` | Secret key for session cookie signing (must be cryptographically random) | _required_ |
| `FRONTEND_URL` | Frontend URL(s) for CORS. Comma-separated for multiple origins | `http://localhost:3000` |
| `API_BASE_URL` | Self-referencing API base URL (used in email links) | — |
| `COMPANY_LOGO_FILENAME` | Logo filename in `/static/images/` for emails | `logo.png` |

### Timeout Tuning
| Variable | Description | Default |
|---|---|---|
| `DMS_GROUP_REFRESH_TIMEOUT_SECONDS` | Timeout for DMS group membership refresh on session restore | `5` |
| `AUTH_DB_TIMEOUT_SECONDS` | Timeout for database lookups during authentication | `5` |
| `WSDL_LOAD_TIMEOUT_SECONDS` | Timeout for initial WSDL schema loading | `10` |
| `WSDL_OPERATION_TIMEOUT_SECONDS` | Timeout for individual SOAP operation calls | `15` |
| `PROCESSING_TASK_TIMEOUT_SECONDS` | Maximum time per AI processing task before timeout | `300` (5 min) |

### ChromaDB
| Variable | Description | Default |
|---|---|---|
| `CHROMA_DB_PATH` | Path to the ChromaDB persistent storage directory | `./chroma_db` |
| `CHROMA_COLLECTION_NAME` | Name of the ChromaDB collection | `edms_documents` |

---

## Setup & Installation

### Prerequisites
- **Python 3.10+**
- **Oracle Instant Client** libraries (or `oracledb` thin mode)
- **FFmpeg** (optional, for hardware-accelerated video watermarking)

### Steps

1. **Create a virtual environment:**
   ```bash
   python -m venv venv
   ```

2. **Activate the environment:**
   ```bash
   # Windows
   venv\Scripts\activate

   # Linux/macOS
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment:** Copy and fill in the `.env` file with your credentials and service URLs.

5. **Run the API:**
   ```bash
   python app.py
   ```
   Or use the provided batch file on Windows:
   ```bash
   setup.bat      # First-time setup (creates venv + installs dependencies)
   run_api.bat    # Start the API server (Uvicorn with 4 workers)
   ```

### Offline / Air-Gapped Installation

For servers without internet access:

1. **On a connected machine:** Run `download_packages.bat` to download all pip wheels into `/packages/`.
2. **Copy** the entire project folder (with `/packages/`) to the air-gapped server.
3. **On the server:** Run `install_packages.bat` to install from the local wheel cache.

The API starts on `http://localhost:5000` by default.

---

## API Routes

### Authentication & Users

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/auth/login` | Authenticates user via DMS SOAP service, creates session. Rate limited: 5/min |
| `POST` | `/api/auth/logout` | Clears the user session |
| `GET` | `/api/auth/user` | Returns current authenticated user details (refreshes group membership & tab permissions) |
| `PUT` | `/api/user/language` | Updates user language preference (`en` / `ar`) |
| `PUT` | `/api/user/theme` | Updates user UI theme preference (`light` / `dark`) |
| `GET` | `/api/groups` | Lists DMS groups. Admins/supervisors see all groups; regular users see only their own |
| `GET` | `/api/groups/{group_id}/members` | Lists members of a specific DMS group |
| `GET` | `/api/groups/search_members` | Searches for users within a specific DMS group |
| `GET` | `/api/document/{doc_id}/trustees` | Returns the security trustees (access control list) for a document |

**Authentication Flow:**
1. User submits credentials → API authenticates against SOAP DMS → DMS returns a session token (DST)
2. API checks that the user exists in the local Smart EDMS database
3. API fetches the user's DMS group memberships to determine security level (Admin/Editor/Viewer)
4. Group membership flags (EMS Admin, DOCS Supervisors) are cached in the session
5. Security level, tab permissions, and DMS token are stored in a server-side session cookie
6. All subsequent requests use the session cookie for authentication
7. On session restore (`GET /api/auth/user`), group memberships are refreshed with a configurable timeout fallback

### Documents

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/documents` | Fetches paginated, filtered documents. Supports search (text + vector), date range, person/tag filters, media type, year, sort, scope, and "memories" mode |
| `POST` | `/api/upload_document` | Uploads a document to DMS with metadata. Validates file quota, extracts EXIF date, inherits parent folder security, sanitizes input. Rate limited: 10/min |
| `POST` | `/api/process_uploaded_documents` | Enqueues newly uploaded documents for AI processing |
| `POST` | `/process-batch` | Fetches all unprocessed documents from Oracle and enqueues them for AI processing. Rate limited: 3/min |
| `GET` | `/api/document/{docnumber}` | Streams a document file directly from DMS for inline viewing |
| `PUT` | `/api/update_metadata` | Updates a document's abstract and/or date taken |
| `GET` | `/api/download_watermarked/{doc_id}` | Downloads a document with a digital watermark (user system ID + timestamp) applied. Images, PDFs, and videos get watermarks; other files stream directly |
| `POST` | `/api/update_abstract` | Updates a document's abstract with detected VIP names |
| `PUT` | `/api/document/{doc_id}/event` | Links a document to an event |
| `GET` | `/api/document/{doc_id}/event` | Gets the event linked to a document |
| `POST` | `/api/document/{doc_id}/security` | Sets the trustees (access control) for a document or folder |

### Media

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/image/{doc_id}` | Serves a raw image from DMS |
| `GET` | `/api/pdf/{doc_id}` | Serves a raw PDF from DMS |
| `GET` | `/api/video/{doc_id}` | Streams a video with caching — uses disk cache for repeat views, otherwise streams from DMS and writes to cache simultaneously |
| `GET` | `/api/temp_thumbnail/{doc_id}` | Generates and serves a temporary thumbnail (24h TTL) for a document |
| `GET` | `/cache/{filename}` | Serves a cached thumbnail from the persistent cache |
| `POST` | `/api/clear_cache` | Clears all thumbnail and video caches (Editor+ required) |
| `GET` | `/api/media_counts` | Returns counts of images, videos, and files (filtered by app source and scope) |

### Folders

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/folders` | Lists folder contents (subfolders + documents) with optional filtering by scope, media type, search term |
| `POST` | `/api/folders` | Creates a new folder in DMS. Inherits parent folder security and grants creator full control |
| `PUT` | `/api/folders/{folder_id}` | Renames a folder |
| `DELETE` | `/api/folders/{folder_id}` | Deletes a folder. If it has referenced child items, automatically performs recursive force-delete. Restores user quota for deleted files |
| `POST` | `/api/folders/move-items` | Moves multiple items (documents/folders) to a new parent folder |
| `GET` | `/api/folders/{folder_id}/download-zip` | Downloads all direct files in a folder as a ZIP archive. Enforces 300 MB size limit |

### Tags & Persons

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/tags` | Fetches all tags, respecting security level filtering and app source |
| `GET` | `/api/tags/{doc_id}` | Fetches tags for a specific document |
| `POST` | `/api/tags/batch` | Fetches tags for multiple documents in a single request (eliminates N+1 queries). Limited to 50 docs |
| `POST` | `/api/tags/{doc_id}` | Adds a new tag to a document (auto-translates between EN↔AR) |
| `DELETE` | `/api/tags/{doc_id}/{tag}` | Removes a tag from a document |
| `POST` | `/api/tags/shortlist` | Toggles a tag's shortlisted status (Editor+ required) |
| `POST` | `/api/add_person` | Adds a new person to the lookup table (auto-translates name) |
| `GET` | `/api/persons` | Searches persons with pagination |
| `POST` | `/api/processing_status` | Checks if documents are still being AI-processed |

### Events

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/events` | Lists events with pagination and search. Supports `fetch_all` mode for dropdowns |
| `POST` | `/api/events` | Creates a new event |
| `GET` | `/api/events/{event_id}/documents` | Gets documents linked to an event with pagination |
| `GET` | `/api/journey` | Returns aggregated timeline/journey data |

### Favorites

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/favorites/{doc_id}` | Adds a document to user's favorites |
| `DELETE` | `/api/favorites/{doc_id}` | Removes a document from user's favorites |
| `GET` | `/api/favorites` | Lists the user's favorited documents with pagination |

### Memories

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/memories` | Returns "On This Day" documents — photos/videos from the same month/day in previous years. Supports month/day/limit parameters |

### Sharing

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/share/generate` | Generates shareable links for documents or folders. Supports **open mode** (any `@rta.ae` email) and **restricted mode** (specific email recipients). Sends notification emails to restricted recipients. Rate limited: 100/min |
| `GET` | `/api/share/info/{token}` | Returns share link metadata (restricted vs open, expiry, share type) without authentication |
| `POST` | `/api/share/request-access/{token}` | Step 1 of OTP flow: validates email domain/target, generates a 6-digit OTP, stores it in DB, and sends via SMTP. Rate limited: 5/min |
| `POST` | `/api/share/verify-access/{token}` | Step 2 of OTP flow: verifies OTP. Returns document info (file shares) or folder info (folder shares). Logs access |
| `GET` | `/api/share/folder-contents/{token}` | Returns shared folder contents with subfolder navigation. Validates the requested folder is within the shared hierarchy |
| `GET` | `/api/share/stream/{token}` | Streams a shared document for inline viewing (no watermark). Optimized for video with Range request support via cache |
| `GET` | `/api/share/download/{token}` | Downloads a shared document with a watermark containing the viewer's email. Rate limited: 10/min |

**Sharing Security Model:**
- **Open shares**: Any `@rta.ae` email can request OTP access
- **Restricted shares**: Only the designated email can access; auto-sends notification with the link
- **Folder shares**: Validates every subfolder/document access against the root shared folder hierarchy
- **OTP verification**: Required for all shares — 6-digit code sent via email, stored in Oracle DB with expiry
- **Watermarking on download**: All downloaded files are watermarked with the viewer's email + timestamp + doc ID

### Admin Panel

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/admin/check-access` | Checks if the user is on the admin allowlist |
| `GET` | `/api/admin/users` | Paginated list of EDMS users with search |
| `POST` | `/api/admin/users` | Adds a new user to Smart EDMS (from the PEOPLE table) |
| `PUT` | `/api/admin/users/{id}` | Updates user security level, language, theme, and quota |
| `DELETE` | `/api/admin/users/{id}` | Removes a user from Smart EDMS |
| `GET` | `/api/admin/security-levels` | Lists available security levels |
| `GET` | `/api/admin/search-people` | Searches Oracle PEOPLE table for users not yet in Smart EDMS |
| `GET` | `/api/admin/processing-queue/status` | Returns queue status (queued/in-progress/completed/failed counts), recent failures, Oracle pending count, worker mode |
| `POST` | `/api/admin/processing-queue/worker/pause` | Pauses the background processing worker |
| `POST` | `/api/admin/processing-queue/worker/resume` | Resumes the background processing worker |
| `POST` | `/api/admin/processing-queue/worker/drain` | Gracefully drains: finishes current jobs, then pauses |
| `POST` | `/api/admin/processing-queue/retry-failed` | Retries all failed processing jobs (resets Oracle attempts) |
| `POST` | `/api/admin/processing-queue/retry-selected` | Retries specific failed jobs by document number |
| `DELETE` | `/api/admin/processing-queue/completed` | Purges completed jobs older than N hours |
| `GET` | `/api/admin/tab-permissions/{user_id}` | Gets tab permissions for a specific user |
| `PUT` | `/api/admin/tab-permissions` | Creates or updates a tab permission |
| `POST` | `/api/admin/tab-permissions/init/{user_id}` | Creates default tab permissions for a new user |
| `DELETE` | `/api/admin/tab-permissions/{id}` | Deletes a specific tab permission |

### eDOCS Admin

Server-side eDOCS DM cache management. Clears metadata, index, echo, and temporary cache files from the OpenText eDOCS local cache directory (`%APPDATA%/OpenText/DM/Cache/`). Requires admin allowlist access.

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/admin/edocs/clear-cache` | Triggers a passive eDOCS server cache clear as a background task. Returns `202 Accepted` immediately. Optionally accepts a `user_id` to scope the operation. Skips locked files to preserve active sessions |

**Cache Clear Behavior:**
- Targets specific cache zones: `Default`, `metadata`, `index`, `echo`, `temp`, `document`
- **Passive/non-destructive**: Does not kill processes, close applications, or force-delete locked files
- Cleans files recursively within target zones, then removes empty directories
- Returns detailed statistics: deleted files count, skipped locked files, removed directories

### EMS Admin

EMS Admin manages the organizational hierarchy: **Agencies → Departments → Sections**. Access requires EMS_ADMIN group membership or Editor/Admin security level.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/ems-admin/check-access` | Checks EMS admin access |
| `GET` | `/api/departments/agencies` | Lists all active agencies |
| `GET` | `/api/sections` | Lists companies/sections with pagination and search |
| `POST` | `/api/sections/add` | Adds a new company/section |
| `PUT` | `/api/sections/update` | Updates a company/section |
| `GET` | `/api/departments` | Lists departments with pagination, search, and agency filter |
| `POST` | `/api/departments/add` | Adds a new department |
| `PUT` | `/api/departments/update` | Updates a department |
| `GET` | `/api/ems_sections/departments_by_agency/{id}` | Gets departments filtered by agency |
| `GET` | `/api/ems_sections` | Lists EMS sections with filters |
| `POST` | `/api/ems_sections/add` | Adds a new EMS section |
| `PUT` | `/api/ems_sections/update` | Updates an EMS section |

### EDMS People Management

Full CRUD for DMS user accounts in the PEOPLE table, including group memberships, network aliases, and HR system linkage. Access requires EMS Admin privileges.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/edms-people` | Paginated list of EDMS users (PEOPLE table) with HR hierarchy data. Supports search by username, full name, or email |
| `GET` | `/api/edms-people/hr-employees` | Searches HR employees (`LKP_HR_EMPLOYEES`) not yet provisioned in EDMS. Returns up to 50 results |
| `GET` | `/api/edms-people/hr/agencies` | Lists all active agencies for HR dropdown selection |
| `GET` | `/api/edms-people/hr/departments` | Lists departments filtered by agency for cascading dropdowns |
| `GET` | `/api/edms-people/hr/sections` | Lists sections filtered by department for cascading dropdowns |
| `GET` | `/api/edms-people/groups` | Lists all EDMS groups (`GROUPS` table) for group assignment dropdowns |
| `GET` | `/api/edms-people/{system_id}/details` | Returns detailed info for a user: group memberships, network aliases, and HR hierarchy (agency/department/section) |
| `POST` | `/api/edms-people` | Creates a new EDMS user: inserts into PEOPLE, assigns groups via PEOPLEGROUPS, creates network aliases, auto-assigns DOCS_USERS group, and marks the HR employee record as provisioned |
| `PUT` | `/api/edms-people/{system_id}` | Updates an existing EDMS user: modifies PEOPLE fields, rebuilds group memberships, rebuilds network aliases. Password update is optional |
| `GET` | `/api/edms-people/export` | Exports all active EDMS users with full details (username, name, email, HR hierarchy, group memberships, last login) for Excel download. Filters disabled users |

**User Provisioning Flow:**
1. Admin searches HR employees not yet in EDMS → selects one
2. System auto-fills username, name, email from HR record
3. Admin sets password, primary group, additional groups, security level, and network aliases
4. On create: system generates a new SYSTEM_ID via Oracle sequence, hashes the password (MD5 Base64), inserts the PEOPLE record, assigns all groups (including auto-assigning `DOCS_USERS`), creates network aliases, and marks the HR record with `IS_EDMS_USR = 'Y'`

### Profile Search

Advanced document search that queries Oracle database tables dynamically based on configured search scopes and types.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/profilesearch/scopes` | Returns available search scopes for the current user (cached 5 min) |
| `GET` | `/api/profilesearch/types` | Returns available search types, optionally filtered by scope (cached 5 min) |
| `POST` | `/api/profilesearch/search` | Multi-criteria search: accepts up to 6 criteria (AND logic), scope, date range, and pagination. Dynamically builds SQL against configured Oracle tables |
| `GET` | `/api/profilesearch/search` | Legacy single-criterion search (backwards compatible) |

**Search Scopes:**
- **0 (Global)**: Cross-table search across all associated Oracle forms — slower but comprehensive
- **Specific Form IDs** (e.g., 2572, 3799): Queries only the specific form's Oracle table — significantly faster for targeted searches

---

## AI Processing Pipeline

When documents are uploaded or a batch process is triggered, the **Processor Service** (`services/processor.py`) orchestrates AI analysis. All synchronous AI client calls are offloaded to a thread pool via `run_in_threadpool()` to avoid blocking the async event loop.

### For Images:
1. **Image Captioning** → Generates natural language descriptions + tags (via streaming upload)
2. **OCR** → Extracts text from images (Arabic + English, via streaming upload)
3. **Face Recognition** → Detects and identifies known individuals (VIPs, via streaming upload)
4. Tags are auto-translated (EN↔AR) and inserted into the database
5. The abstract is enriched with Caption, OCR text, and VIP names

### For Videos:
1. **Video Summarization** → Analyzes frames for objects, extracts audio transcript, detects faces, performs OCR on frames (async polling with 30-min timeout)
2. **Face Recognition** → Identifies faces from extracted base64-encoded frames
3. **Transcript Tokenization** → Extracts keywords from audio transcript (via SSE streaming)
4. Tags are auto-translated and inserted
5. The abstract is enriched with all findings

### For PDFs:
1. **PDF OCR** → Extracts text from all pages (via streaming upload)
2. **Tokenization** → Extracts keywords from the OCR text (via SSE streaming)
3. Tags are auto-translated and inserted

### Processing Status Codes:
- **1** = In progress / partially complete (will be retried)
- **2** = Failed (max 3 attempts)
- **3** = Fully complete

---

## Background Processing Queue

The processing queue (`services/processing_queue.py`) is a **SQLite-backed local job queue** that manages AI processing jobs.

### Queue Architecture:
- **SQLite database** (WAL mode, 5-second busy timeout) stores jobs with status tracking
- **Unique constraint** on `docnumber` prevents duplicate job entries
- **Index** on `(status, available_at, updated_at)` for efficient job claiming
- **Async worker loop** continuously claims and processes queued jobs
- **3 concurrent jobs** are claimed per worker cycle
- **5-minute task timeout** (configurable via `PROCESSING_TASK_TIMEOUT_SECONDS`) prevents hung tasks from blocking the queue
- **Automatic retry** with 20-second delay for transient failures
- **Max 3 attempts** per document before marking as permanently failed
- **Error truncation**: Error messages are capped at 2,000 characters

### Worker Modes:
- **Running**: Actively claiming and processing jobs
- **Paused**: Not claiming new jobs (default on startup)
- **Draining**: Finishing current jobs, then auto-transitioning to paused

### Job Lifecycle:
```
queued → in_progress → completed
                    → failed (after 3 attempts)
                    → queued (retried with delay)
```

### Worker Mode Audit:
All mode transitions are recorded in a `worker_mode_audit` SQLite table with:
- Previous mode, new mode, actor (system/admin), reason, and ISO timestamp

---

## Worker Process

Background AI processing runs in a **dedicated standalone process** (`worker.py`), separate from the FastAPI web server. This architectural decision keeps request handling isolated from heavy CPU/IO-bound AI processing work.

### Running the Worker:
```bash
python worker.py
```

### Worker Behavior:
1. Continuously polls the SQLite queue for available jobs
2. Claims up to 3 jobs per cycle
3. Authenticates against DMS using system credentials (for document retrieval)
4. For each job: fetches document from Oracle, calls `process_document()` with a timeout
5. On success: marks job as completed in both SQLite and Oracle
6. On timeout: marks job as failed
7. On transient error: requeues with a 20-second delay and incremented attempt counter
8. Supports graceful shutdown via `KeyboardInterrupt`

---

## Vector Search (ChromaDB)

The API maintains a **ChromaDB** persistent vector store for semantic document search:

- **Collection**: `edms_documents` — stores document abstracts as embeddings
- **Embedding**: Uses an external embedding service (MiniLM L6 v2, 384 dimensions) via `ExternalEmbeddingFunction` — a custom wrapper that implements ChromaDB's `EmbeddingFunction` interface
- **Distance Threshold**: Results with distance > 1.3 are filtered out as irrelevant
- **Lazy Initialization**: ChromaDB client is created on first use, not at import time (thread-safe via GIL)
- **Telemetry Disabled**: ChromaDB's anonymized telemetry is explicitly disabled
- **Backfill Script**: `backfill_vectors.py` bulk-indexes existing Oracle documents into ChromaDB in batches of 100, with automatic reconnection on database errors
- **CRUD**: Supports `add_or_update_document()`, `query_documents()`, and `delete_document()`

When users search, the API queries both the traditional Oracle full-text search AND the ChromaDB vector search, merging results for hybrid semantic+keyword search.

---

## WSDL / SOAP Client

The `wsdl_client/` package provides a Python adapter over the on-premise DMS (Document Management System) SOAP service:

| Module | Functions |
|---|---|
| **auth.py** | `dms_user_login()` — authenticates users; `dms_system_login()` — authenticates with system credentials for background operations (30-minute TTL cache) |
| **documents.py** | `upload_document_to_dms()`, `get_document_from_dms()`, `get_image_by_docnumber()`, `stream_document_from_dms()`, `delete_document()`, `set_trustees()`, `get_object_trustees()` |
| **folders.py** | `create_dms_folder()`, `list_folder_contents()`, `rename_folder_display()`, `move_item_to_parent()`, `delete_folder_contents()` |
| **users.py** | `get_groups_for_user()`, `get_all_groups()`, `get_group_members()`, `search_users_in_group()` |
| **utils.py** | DMS binary buffer parsing: `parse_dms_buffer()`, `parse_user_result_buffer()`, `parse_groups_buffer()`, `parse_group_members_buffer()`, `parse_binary_result_buffer()` |

### Binary Buffer Parsing (`wsdl_client/utils.py`)

The DMS SOAP service returns result data in a proprietary binary format: base64-encoded, optionally zlib-compressed, with UTF-16LE length-prefixed strings. The `utils.py` module implements a robust multi-strategy parser that:

1. **Decompression**: Tries 6 different zlib decompression strategies (custom DMS `0xFFFFFFFF` marker, standard zlib headers, raw deflate, various header offsets)
2. **String Extraction**: Reads length-prefixed UTF-16LE strings with null terminators
3. **Format Detection**: Automatically detects whether data has a row/column header or requires metadata-driven parsing
4. **User Normalization**: Heuristically determines which columns are USER_ID vs FULL_NAME based on content patterns (spaces, case, digits)
5. **Folder Parsing**: Extracts folder/file items from binary buffers, identifying item types (Folder/Node) and media types from file extensions

---

## Database Layer

The `database/` package contains all Oracle and SQLite query logic:

### Key Modules:

| Module | Purpose |
|---|---|
| **connection.py** | Oracle connection pooling (async and sync), `get_async_connection` wrapper, best-effort performance index creation at startup (30-second timeout), blocklist loading from `blocklist.json`, custom `DatabaseConnectionError` exception |
| **documents.py** | Complex paginated document fetch with 10+ filter dimensions, vector search integration, processing status tracking with robust transaction handling, abstract/metadata updates |
| **media.py** | DMS media operations: file info resolution, content streaming (sync + async), thumbnail generation (image/PDF/video), media type detection from file extensions, EXIF date extraction, cache path helpers, app-bucket caching |
| **tags.py** | Full tag lifecycle: create keywords (EN+AR), link to documents, batch fetch (up to 50 docs), shortlist toggle, security-level-aware filtering, tag translation & update |
| **events.py** | Event CRUD, document-event joins, journey data aggregation |
| **favorites.py** | Per-user favorites CRUD with pagination |
| **memories.py** | "On This Day" queries, journey data aggregation |
| **users.py** | User details fetch, security level queries, language/theme preference updates, system_id lookup |
| **user_data.py** | User quota management: allocation, deduction on upload, restoration on delete; EDMS user ID resolution |
| **groups.py** | Synchronous DMS group queries (fallback when async is unavailable) |
| **profilesearch.py** | Dynamic SQL builder that constructs Oracle queries based on configured form fields, supporting LIKE/exact/startsWith matching, date ranges, multi-criteria AND logic, and fallback hardcoded types |
| **sharing.py** | Share link CRUD, OTP generation & verification, access logging, email target validation |
| **admin.py** | Admin-facing user management: add/delete/update EDMS users, security level listing, people search, document history |
| **ems_admin.py** | EMS organizational data: agencies, departments, sections — CRUD with duplicate checking and sequence-based ID generation |
| **edms_people.py** | Full EDMS user lifecycle: PEOPLE table CRUD, PEOPLEGROUPS rebuild, NETWORK_ALIASES management, HR employee linkage, password hashing, DOCS_USERS auto-assignment, export with group aggregation |
| **tab_permissions.py** | Per-user tab visibility: CRUD, default permission initialization, admin full-permissions view |

---

## Utilities

### Watermarking (`utils/watermark.py`)
Applies digital watermarks to downloaded files:
- **Images**: Pillow-based text overlay (bottom-right, semi-transparent)
- **PDFs**: PyMuPDF text insertion on every page
- **Videos**: FFmpeg with hardware acceleration (NVIDIA/AMD/Intel QSV) → MoviePy fallback. Watermark appears at start, end, and random intervals throughout the video. Includes automatic encoder detection and MoviePy FFmpeg path configuration

### Input Sanitization (`utils/sanitize.py`)
- Strips HTML tags and dangerous characters from user input
- Prevents stored XSS in document names and abstracts

### Cache Eviction (`utils/cache_eviction.py`)
- Runs every 6 hours as a background task
- Evicts old video cache files based on file age (LRU policy)

### Log Cleanup (`utils/log_cleanup.py`)
- Runs every 24 hours as a background task
- Deletes IIS/application log files older than 14 days from the `logs/` directory
- Safely skips files that are locked/in-use by IIS
- Returns statistics: deleted count and bytes freed

### TTL Cache (`utils/ttl_cache.py`)
- In-memory cache with configurable TTL (default 5 minutes)
- Single-process deployment optimized
- Used for profile search scope/type metadata to avoid redundant DB queries

### Common Utilities (`utils/common.py`)
- **`get_current_user()`**: FastAPI dependency for session-based authentication
- **`get_mimetype_for_media()`**: Centralized MIME type mapping for 15+ file types
- **`build_content_disposition()`**: RFC 5987-compatible Content-Disposition header builder with UTF-8 filename support and ASCII fallback
- **`load_company_logo_base64()`**: Loads company logo from multiple search paths and returns as base64 data URI for email embedding
- **`get_otp_email_template()`**: HTML email template for OTP verification codes
- **`get_share_link_email_template()`**: HTML email template for share link notifications
- **`send_otp_email()`**: Sends OTP verification emails via SMTP with HTML + plain text fallback
- **`send_share_link_email()`**: Sends share link notification emails via SMTP
- **`clean_repeated_words()`**: Deduplicates repeated words in AI-generated text

---

## Middleware Stack

The application applies the following middleware (in order of execution):

### 1. Security Headers Middleware (`SecurityHeadersMiddleware`)
- **Content-Security-Policy**: Restricts resource loading to `'self'` with necessary inline script/style exceptions
- **X-Content-Type-Options**: `nosniff`
- **X-Frame-Options**: `DENY` (except for `/api/pdf/` routes, which are designed for iframe embedding)
- **Referrer-Policy**: `strict-origin-when-cross-origin`

### 2. CORS Middleware
- Configured via `FRONTEND_URL` environment variable
- Supports multiple origins (comma-separated)
- Credentials enabled for cookie-based sessions
- Allowed methods: GET, POST, PUT, DELETE, OPTIONS
- Allowed headers: Content-Type, Authorization, X-App-Source, Cookie

### 3. Session Middleware
- Starlette SessionMiddleware with cookie-based sessions
- 60-day session expiry (`max_age=5184000`)
- `SameSite=Lax` for CSRF protection
- Session cookie name: `session`

### 4. Request Metrics Middleware (`RequestMetricsMiddleware`)
- Generates or propagates `X-Request-ID` header for request tracing
- Measures request duration in milliseconds
- Captures: method, path, status code, duration, and authenticated username
- Structured JSON logging (currently disabled but wired up for PostHog/observability)

---

## Security

### Authentication
- **DMS SOAP authentication** validates user credentials against the enterprise DMS
- Server-side **session cookies** (60-day expiry, `SameSite=Lax`)
- All API routes (except health check and share info) require authentication via `get_current_user` dependency
- Session restore includes a timeout-protected group membership refresh

### Authorization
- **Three-tier security model**: Admin (9), Editor (5), Viewer (0)
- Security level determined by DMS group membership at login
- **Admin allowlist** for admin panel access (hardcoded usernames)
- **EMS Admin group** for organizational management and EDMS people management access
- **DOCS Supervisors group** for elevated visibility
- **Per-user tab permissions** for fine-grained UI access control
- **Document-level security** via DMS trustees (ACL)

### Rate Limiting
| Endpoint | Limit |
|---|---|
| Login | 5 requests/minute per IP |
| Upload | 10 requests/minute per IP |
| Batch process | 3 requests/minute per IP |
| Share link generation | 100 requests/minute per IP |
| OTP request | 5 requests/minute per IP |
| Share download | 10 requests/minute per IP |

### Security Headers
- Content Security Policy (CSP)
- X-Content-Type-Options: nosniff
- X-Frame-Options: DENY (except PDF embeds)
- Referrer-Policy: strict-origin-when-cross-origin

### CORS
- Configured via `FRONTEND_URL` environment variable
- Supports multiple origins (comma-separated)
- Credentials enabled for cookie-based sessions

---

## Deployment

### Development
```bash
python app.py
```

### Production (Uvicorn with Workers)
```bash
uvicorn app:app --host 0.0.0.0 --port 5000 --workers 4
```
Or use the provided batch file:
```bash
run_api.bat
```

### Production (IIS with HttpPlatformHandler)
The `web2.config` file configures deployment on Windows IIS:
- Uses `HttpPlatformHandler` to proxy requests to Uvicorn
- Configures environment variables including all timeout tuning parameters
- Enables stdout logging to `./logs/python-stdout.log`
- 300-second startup timeout, 20-minute request timeout
- 4 processes per application

### Background Worker
The processing worker must be started separately:
```bash
python worker.py
```
In production, this should be run as a Windows service or supervised process.

---

## Batch Scripts & Tooling

| Script | Purpose |
|---|---|
| `setup.bat` | First-time setup: creates virtual environment and installs dependencies |
| `run_api.bat` | Starts the API server with Uvicorn (4 workers, host `0.0.0.0`, port `5000`) |
| `download_packages.bat` | Downloads all pip wheels into `/packages/` for offline deployment |
| `install_packages.bat` | Installs packages from the local `/packages/` wheel cache (no internet required) |
| `create_archive.bat` | Creates a timestamped ZIP archive of the project, respecting `.gitignore` patterns and excluding `venv/`, `.git/`, and the script itself |

---

## Blocklist & Content Filtering

The `blocklist.json` file provides content filtering rules used during tag processing and text analysis:

| Category | Purpose |
|---|---|
| **characters** | Individual characters (A-Z, 0-9, symbols, Arabic letters) that are too short to be meaningful tags |
| **meaningless_english** | English stop words (articles, prepositions, pronouns, common verbs) filtered from AI-generated tags |
| **meaningless_arabic** | Arabic stop words filtered from AI-generated tags |
| **profanity_english** | English profanity words blocked from tags |
| **profanity_arabic** | Arabic profanity words blocked from tags |

This file is loaded at startup via `database/connection.py` and used throughout the tag processing pipeline to ensure only meaningful, appropriate tags are stored.
