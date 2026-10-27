# Smart EDMS — Backend API (Central Orchestrator)

[![Platform](https://img.shields.io/badge/Platform-Windows%20Server-0078D6?logo=windows)](https://www.microsoft.com/windows-server)
[![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-FastAPI%20(ASGI)-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![ASGI Server](https://img.shields.io/badge/ASGI%20Server-Uvicorn%20(4%20Workers)-499848?logo=gunicorn&logoColor=white)](https://www.uvicorn.org/)
[![Database](https://img.shields.io/badge/Database-Oracle%20Enterprise%2019c-F80000?logo=oracle&logoColor=white)](https://www.oracle.com/database/)
[![DMS](https://img.shields.io/badge/DMS-OpenText%20eDOCS-005B94?logo=opentext)](https://www.opentext.com/)
[![Vector Store](https://img.shields.io/badge/Vector%20Store-ChromaDB%20(4096--dim)-FF6B6B)](https://www.trychroma.com/)
[![AI Infrastructure](https://img.shields.io/badge/AI%20Cloud-GovAI%20LLM%20%26%20Dataiku%20AI%20Center-6366F1)](https://llmapi.govai.ae)

The **Smart EDMS API** is the central orchestrator and middleware hub for the **RTA Smart EDMS** (Electronic Document Management System) ecosystem. Built with **FastAPI** and served via **Uvicorn** (under **Windows Server IIS** with `HttpPlatformHandler`), it unifies enterprise web and mobile client applications, on-premise **OpenText eDOCS DM** repository storage, an **Oracle Enterprise Database**, a local **ChromaDB** dense vector store, an asynchronous **SQLite job processing queue**, and a suite of specialized **AI document intelligence microservices**.

Neither client frontends nor downstream AI services interact directly with the underlying Oracle database or OpenText eDOCS repositories. All authentication, security permissions, document streaming, EXIF extraction, digital watermarking, organizational hierarchy management, and background AI extraction pipelines are mediated through this API.

---

## 📑 Table of Contents

1. [Executive Summary & Architectural Role](#1-executive-summary--architectural-role)
2. [System Architecture & Data Pipelines](#2-system-architecture--data-pipelines)
   - [2.1 High-Level Architecture Diagram](#21-high-level-architecture-diagram)
   - [2.2 Ingestion & Upload Pipeline](#22-ingestion--upload-pipeline)
   - [2.3 Hybrid Search Pipeline (Semantic Vector + Lexical RRF)](#23-hybrid-search-pipeline-semantic-vector--lexical-rrf)
3. [Connected Ecosystem & Sibling Microservices](#3-connected-ecosystem--sibling-microservices)
   - [3.1 Ecosystem Architecture & Port Reference](#31-ecosystem-architecture--port-reference)
   - [3.2 User Frontends](#32-user-frontends)
   - [3.3 AI Microservices Suite](#33-ai-microservices-suite)
   - [3.4 Enterprise Automation & Task Jobs](#34-enterprise-automation--task-jobs)
   - [3.5 Enterprise Storage & Core Platforms](#35-enterprise-storage--core-platforms)
4. [Technology Stack](#4-technology-stack)
5. [Codebase Structure & Directory Layout](#5-codebase-structure--directory-layout)
6. [API Endpoints Reference](#6-api-endpoints-reference)
   - [6.1 Authentication & User Sessions](#61-authentication--user-sessions)
   - [6.2 Documents & Ingestion](#62-documents--ingestion)
   - [6.3 Media Streaming & Caching](#63-media-streaming--caching)
   - [6.4 Folders & Hierarchy Navigation](#64-folders--hierarchy-navigation)
   - [6.5 Tags, Persons & VIP Recognition](#65-tags-persons--vip-recognition)
   - [6.6 Events & Timeline Journeys](#66-events--timeline-journeys)
   - [6.7 Favorites & Memories ("On This Day")](#67-favorites--memories-on-this-day)
   - [6.8 Secure Sharing & OTP Flow](#68-secure-sharing--otp-flow)
   - [6.9 System Administration & Queue Management](#69-system-administration--queue-management)
   - [6.10 eDOCS Server Cache Administration](#610-edocs-server-cache-administration)
   - [6.11 EMS Organizational Administration](#611-ems-organizational-administration)
   - [6.12 EDMS People Management & HR Provisioning](#612-edms-people-management--hr-provisioning)
   - [6.13 Dynamic Profile Search](#613-dynamic-profile-search)
   - [6.14 Oracle Database Session Monitoring](#614-oracle-database-session-monitoring)
7. [AI Document Processing Pipeline](#7-ai-document-processing-pipeline)
8. [Background Processing Queue & Worker Process](#8-background-processing-queue--worker-process)
9. [Vector Search Engine (ChromaDB)](#9-vector-search-engine-chromadb)
10. [OpenText eDOCS WSDL / SOAP Client](#10-opentext-edocs-wsdl--soap-client)
11. [Database Layer & Oracle Connection Management](#11-database-layer--oracle-connection-management)
12. [Utilities & Media Processing](#12-utilities--media-processing)
13. [Middleware Stack & Security Headers](#13-middleware-stack--security-headers)
14. [Security & Access Control Model](#14-security--access-control-model)
15. [Multi-Node & Load-Balanced Deployment](#15-multi-node--load-balanced-deployment)
16. [Offline Setup & Air-Gapped Installation](#16-offline-setup--air-gapped-installation)
17. [Windows IIS Production Deployment (`web2.config`)](#17-windows-iis-production-deployment-web2config)
18. [Environment Variables Reference (`.env`)](#18-environment-variables-reference-env)
19. [Troubleshooting & Operational Runbook](#19-troubleshooting--operational-runbook)

---

## 1. Executive Summary & Architectural Role

The **Smart EDMS API** serves as the central nervous system of the RTA Electronic Document Management System. Its core responsibilities include:

1. **Enterprise DMS Integration**: Translates REST requests from modern web and mobile clients into binary-level SOAP/WSDL transactions supported by the legacy OpenText eDOCS DM server (including proprietary zlib-compressed buffer parsing and length-prefixed UTF-16LE string decoding).
2. **Oracle Database Governance**: Manages connection pooling, transactions, sequence generation, dynamic SQL query generation, quota management, and schema-level synchronization across more than 20 database tables.
3. **AI Microservice Orchestration**: Directs documents to specialized downstream AI services for optical character recognition (OCR), visual image captioning, VIP facial identification, video summarization, keyword tokenization, and vector embeddings.
4. **Decoupled Architecture**: Strictly isolates compute-heavy deep learning inference from user-facing web requests. Heavy machine learning tasks are handed off to an asynchronous SQLite queue and processed in a dedicated worker process (`worker.py`).
5. **Dual Ingestion Paths**: Accommodates both synchronous, immediate end-user uploads through the portal UI and automated, scheduled batch ingestion via the **EDMS Crawler Task** to prevent compute bottlenecks and external API rate limiting.
6. **Air-Gapped Government Readiness**: Completely self-contained with offline wheel caches (`packages/`), local neural model wrappers, automated batch installers, and hardened IIS configuration for zero-internet production servers.

---

## 2. System Architecture & Data Pipelines

### 2.1 High-Level Architecture Diagram

The system operates across two primary operational pipelines: the **Upload & Ingestion Pipeline** and the **Search Pipeline**.

![Smart EDMS Architecture](diagrams/smart_edms_architecture.jpg)

---

### 2.2 Ingestion & Upload Pipeline

When documents, field photographs, scanned records, or media attachments enter the Smart EDMS platform:

1. **Dual Ingestion Entrypoints**:
   - **User Uploads File (Real-time Upload)**: End users upload files directly through the [Smart EDMS Web Frontend](file:///d:/projects/new%20EDMS%20py/Smart%20EDMS) or [EDMS Media Frontend](file:///d:/projects/new%20EDMS%20py/EDMS%20Media%20frontend) (`POST /api/upload_document`). The API authenticates the session, verifies user storage quota, inspects MIME types, extracts EXIF creation dates, and sanitizes filenames.
   - **EDMS Crawler (Scheduled Batch Ingestion)**: Automated background crawler tasks ([EDMS Crawler Task](file:///d:/projects/new%20EDMS%20py/Task%20Jobs/EDMS%20Crawler%20Task%20py)) execute on a 30-minute cadence under Windows Task Scheduler. The crawler authenticates (`POST /api/auth/login`) and triggers `POST /process-batch`, pulling all unprocessed or queued records (`STATUS = 0`) from Oracle DB into the local SQLite processing queue without user intervention.
2. **Repository & Metadata Storage**:
   - The API pushes the physical binary file into the **OpenText eDOCS DM Vault** via SOAP/WSDL (`upload_document_to_dms`).
   - The primary document metadata record is committed to **Oracle Enterprise Database** (`PROFILE` table), recording author, document date, file extension, and initial status (`STATUS = 1` - In Progress).
3. **Multi-Service AI Document Intelligence Pipeline**:
   The background worker (`worker.py`) claims jobs from SQLite and routes the media stream:
   - **OCR Service (Port 5004)**: Extracts Arabic and English text from scanned documents and images. Connects upstream to **GovAI Multimodal Vision LLM** (`qwen3-vl-30b`), with local EasyOCR + Gemma 3 fallback.
   - **Image Captioning Service (Port 5001)**: Analyzes visual scenes, generates concise captions, and produces descriptive keyword tags via **GovAI Multimodal Vision LLM** (`qwen3-vl-30b`), with local Ollama fallback.
   - **Video Summarizer (Port 5008)**: Samples keyframes for visual understanding (GovAI `qwen3-vl-30b`), extracts audio tracks with MoviePy, transcribes speech via GovAI Whisper ASR (`whisper_asr`), and forwards extracted face crops.
   - **Face Recognition Service (Port 5002)**: Receives raw images or pre-cropped video faces. Performs local PIL cropping, draws green bounding boxes, queries local ChromaDB (`faces` collection), and normalizes Arabic identities using GovAI LLM.
     > [!IMPORTANT]
     > **Perception Middleware Boundary**: The Face Recognition Service does **not** call GovAI LLM for deep neural inference. Instead, it delegates face detection (RetinaFace) and 4096-dimensional facial embedding computation (VGG-Face) to the **Dataiku Model API (Port 5009 / DSS :13000)**, which runs inside the **Enterprise AI Center** on dedicated compute nodes with pre-cached neural network weights.
4. **Metadata Enrichment & Abstract Aggregation**:
   The API consolidates all AI extracts into a structured document `ABSTRACT`. During this stage, the [Translator Rephraser Service](file:///d:/projects/new%20EDMS%20py/Translator%20Rephraser%20py) (Port 5005 / 5010, `POST /generate`) performs four critical tasks:
   - **Tag Translation**: Translates extracted visual, video, and OCR tags from English to Arabic while stripping all Tashkeel diacritics via regex `[\u064B-\u065F\u0670]`.
   - **Caption Shortening**: Shortens verbose image captions exceeding 300 characters via `task="shorten"`.
   - **Abstract Optimization**: Rephrases composite abstracts exceeding 2,000 characters to comply with OpenText eDOCS column limits.
   - **Speech Transcript Tokenization**: Tokenizes video audio transcripts into structured keyword tags via `task="tokenize"`.
   Bilingual tag pairs (`KEYWORD_ID` in English and `DESCRIPTION` in Arabic) are committed to Oracle tables `KEYWORD` and `LKP_DOCUMENT_TAGS`. The document status is updated to `STATUS = 3` (Complete).
5. **Dense Vector Embeddings (ChromaDB Indexing)**:
   The completed abstract is sent to the [Embedding Service](file:///d:/projects/new%20EDMS%20py/Embedding%20Service) (`POST /embed` on Port 5003), which calls GovAI (`qwen3-embedding-8b`) to generate a 4096-dimensional dense mathematical vector. The vector is committed persistently to **ChromaDB** (`edms_documents` collection) mapped to the document number.

---

### 2.3 Hybrid Search Pipeline (Semantic Vector + Lexical RRF)

When a user executes a natural language search in the interface:

```
User Query (e.g., "photos of electric buses and charging stations in Dubai")
       │
       ▼ (1)
Smart Search Service (Port 5006) ◄──► GovAI LLM Cloud (qwen3-235b-fp16)
       │                               ├── Generates Broad Synonyms (EV, green transport)
       │                               └── Isolates Required Concepts (electric buses AND charging stations)
       ├──────────────────────────────────────────────┐
       ▼ (Expanded Query)                             ▼ (SQL Conditions)
Embedding Service (Port 5003)                  Oracle Enterprise Database
       │                                              │
       ▼ (4096-dim Query Vector)                      │
ChromaDB (edms_documents)                             │
       │                                              │
       ▼ (Vector Document IDs + Cosine Ranks)         ▼ (Exact Metadata Matches + Lexical Scores)
       └──────────────────────┬───────────────────────┘
                              ▼
                 Reciprocal Rank Fusion (RRF)
            1.0 / (60 + VectorRank) + 1.0 / (60 + KeywordRank)
                              │
                              ▼
                Ranked Document Results Returned (< 1.5s)
```

1. **Natural Language Query Understanding**:
   The raw search phrase is dispatched to the [Smart Search Service](file:///d:/projects/new%20EDMS%20py/Smart%20Search%20Service) (`POST /tokenize` on Port 5006) **before** an Oracle database connection is acquired. The service prompts GovAI LLM (`qwen3-235b-fp16`) to break the phrase into:
   - **Expanded Tokens**: Synonyms and related terminology (e.g., `"sea"` ➔ `"water"`, `"ocean"`).
   - **Required Concepts**: Mandatory constraints that the user explicitly indicated must co-occur (e.g., `"cars and plants"`).
2. **Parallel Dual-Path Retrieval**:
   - **Semantic Vector Path**: The enriched query is passed to the [Embedding Service](file:///d:/projects/new%20EDMS%20py/Embedding%20Service) (Port 5003) to generate a query vector. ChromaDB evaluates cosine similarity distance and returns candidate document IDs. Matches exceeding `VECTOR_DISTANCE_THRESHOLD` (e.g., > 1.3) are discarded.
   - **Lexical Oracle SQL Path & Cross-Lingual Linkage**: Dynamic SQL queries execute against `PROFILE`, `KEYWORD`, `LKP_DOCUMENT_TAGS`, and `LKP_PERSON`. Because the Translator Service stored bilingual keyword pairs during ingestion (`KEYWORD_ID` = English, `DESCRIPTION` = Arabic without Tashkeel), Oracle SQL CTEs instantly match queries across both languages simultaneously:
     ```sql
     (UPPER(k.KEYWORD_ID) LIKE UPPER(:token) OR k.DESCRIPTION LIKE :token)
     ```
     This cross-lingual linkage enables instant English-to-Arabic and Arabic-to-English retrieval without per-query translation latency.
3. **Reciprocal Rank Fusion (RRF) Ranking**:
   Results from ChromaDB (`VECTOR_RANK`) and Oracle keyword scoring (`KEYWORD_RANK`) are fused directly inside Oracle SQL using the standard Reciprocal Rank Fusion formula:
   $$\text{Score} = \frac{1.0}{60 + \text{VECTOR\_RANK}} + \frac{1.0}{60 + \text{KEYWORD\_RANK}}$$
   Documents scoring highly across both semantic meaning and lexical keywords rise to the top.
4. **Zero-Downtime Fallback**: If the Smart Search Service, Embedding Service, or ChromaDB are unreachable, the API logs a warning and falls back to standard Oracle SQL `LIKE` search without throwing errors to the user.

---

## 3. Connected Ecosystem & Sibling Microservices

### 3.1 Ecosystem Architecture & Port Reference

The Smart EDMS API acts as the orchestrator binding together the following microservices and utilities across `d:\projects\new EDMS py`:

| Service / Project | Host / Port | Core Technology | Architectural Role |
|:---|:---:|:---|:---|
| **EDMS API (This Project)** | **5000** | FastAPI · Uvicorn · Oracle thin | **Central Orchestrator, Middleware, eDOCS SOAP client & Database Manager** |
| **Image Captioning py** | **5001** | FastAPI · GovAI `qwen3-vl-30b` · Ollama | Multimodal scene description, visual tags, zero-disk byte streaming |
| **Face Recognition py** | **5002** | FastAPI · ChromaDB · Pillow | Face gateway, local PIL crops/annotations, vector search in `faces` |
| **Embedding Service** | **5003** | FastAPI · GovAI `qwen3-embedding-8b` | Generates 4096-dim vector embeddings for document abstracts & queries |
| **OCR py** | **5004** | FastAPI · GovAI `qwen3-vl-30b` · EasyOCR | Multilingual Arabic/English text extraction from scanned documents |
| **Smart Search Service** | **5006** | FastAPI · GovAI `qwen3-235b-fp16` | Natural language intent extraction, synonym expansion, required concepts |
| **Video Context Summarizer**| **5008** | FastAPI · OpenCV · MoviePy · Whisper | Strategic keyframe vision, speech transcription, face crop extraction |
| **Dataiku Model API** | **5009** / DSS :13000 | Flask · Waitress · RetinaFace · VGG-Face | **Stateless ML perception middleware for Face Recognition (AI Center)** |
| **Translator Rephraser py** | **5005** / 5010 | FastAPI · GovAI `qwen3-235b-fp16` | Arabic Tashkeel removal, EN↔AR tag translation, caption/abstract shortening (< 300 / 2,000 ch), transcript tokenization |
| **Smart EDMS Frontend** | **3000** | Next.js 15 · React 19 · TypeScript | Enterprise primary web portal (admin dashboards, viewer, sharing) |
| **EDMS Media Frontend** | **3000** | Next.js 16 · React 19 · TanStack Query | Media viewer, Next.js server-side API proxy (`/api/[...proxy]`) |
| **EDMS Mobile** | Native | React Native · Expo | Mobile viewer application for iOS and Android |
| **EDMS Archiving System** | **5000** / **3000** | Next.js 14 · Flask · Waitress | Public Transport Agency (PTA) compliance & document expiry tracker |
| **Oracle Session Monitor** | Scheduled | Python · PyInstaller | Reaps hanging Oracle connections (> 3h) during weekend maintenance windows |
| **EDMS Crawler Task** | Scheduled | Python · PyInstaller (`EDMSCrawlerTask.exe`) | Scheduled 30-min pacing trigger for `POST /process-batch` |
| **EDMS Auto Check-In Job** | Scheduled | Python · PyInstaller (`edms_auto_checkin.exe`)| Unlocks orphaned checked-out documents in Oracle via eDOCS WSDL |
| **EDMS Inactive Users Job**| Scheduled | Python · PyInstaller (`disable_inactive_users.exe`)| Deactivates dormant accounts (> 90 days) in `PEOPLE`, shields service users |
| **EDMS Restart DM Server** | Scheduled | Python · PyInstaller (`restart_edocs_dm.exe`) | Orderly service cycling (`DMServerMonitor`, `DOCSFusion`, `DMSvrService`) |

---

### 3.2 User Frontends

1. **[Smart EDMS Web Frontend](file:///d:/projects/new%20EDMS%20py/Smart%20EDMS)**: Next.js 15 application providing the full user interface: document collage grids, full-screen media modals, interactive folder navigation, document-event journey timelines, tag editors, VIP managers, and administrative control centers.
2. **[EDMS Media Frontend](file:///d:/projects/new%20EDMS%20py/EDMS%20Media%20frontend)**: Next.js 16 App Router application. Routes all backend requests through a server-side API proxy (`app/api/[...proxy]/route.ts`) with automatic retries, cookie pass-through, and 10-minute timeouts for streaming downloads.
3. **[EDMS Mobile](file:///d:/projects/new%20EDMS%20py/EDMS%20Mobile)**: Native mobile application built with React Native and Expo for tablets and mobile devices.
4. **[EDMS Archiving System](file:///d:/projects/new%20EDMS%20py/EDMS%20Archiving%20system)**: Specialized compliance portal for the Public Transport Agency (PTA) enforcing document validity and expiration date tracking.

---

### 3.3 AI Microservices Suite

- **[Image Captioning Service](file:///d:/projects/new%20EDMS%20py/Image%20Captioning%20py)** (`CAPTIONING_API_URL`): Invoked by `api_client.get_captions()` via memory streaming (`POST /process_image_stream`). Employs GovAI `qwen3-vl-30b` to generate scene descriptions and tags, bounded by `_cap_caption()` to guarantee Oracle column compatibility (< 500 chars).
- **[Face Recognition Service](file:///d:/projects/new%20EDMS%20py/Face%20Recognition%20py)** (`FACE_API_URL`): Invoked by `api_client.recognize_faces()` (`POST /api/analyze_image_stream`) and `api_client.recognize_faces_from_list()` (`POST /api/recognize_faces`). Maintains a local ChromaDB instance (`faces` collection).
- **[Dataiku Model API](file:///d:/projects/new%20EDMS%20py/Dataiku%20Model%20API)** (`DATAIKU_MODEL_API_URL`): High-throughput machine learning inference engine running on Port 5009 or Dataiku DSS (`rtauevaidss1.rtatestdom.local:13000`). It is 100% stateless and executes RetinaFace face detection and VGG-Face 4096-dimensional vector extraction. **Operates as part of the Enterprise AI Center without utilizing GovAI LLM.**
- **[Embedding Service](file:///d:/projects/new%20EDMS%20py/Embedding%20Service)** (`EMBEDDING_API_URL`): Invoked by `api_client.get_embedding_from_service()` (`POST /embed`). Delegates to GovAI `qwen3-embedding-8b` to return 4096-dimensional vectors for abstracts and search queries.
- **[OCR Service](file:///d:/projects/new%20EDMS%20py/OCR%20py)** (`OCR_API_URL`): Invoked by `api_client.get_ocr_text()` (`POST /translate_image_stream`). Performs Arabic & English OCR via GovAI `qwen3-vl-30b` with watermark deduplication, falling back to local OpenCV CLAHE + EasyOCR.
- **[Smart Search Service](file:///d:/projects/new%20EDMS%20py/Smart%20Search%20Service)** (`SMART_SEARCH_API_URL`): Invoked by `api_client.get_search_intent()` (`POST /tokenize`). Prompts GovAI `qwen3-235b-fp16` to extract domain synonyms and logical required concepts.
- **[Video Context Summarizer](file:///d:/projects/new%20EDMS%20py/Video%20Context%20Summarizer)** (`VIDEO_SUMMARIZER_API_URL`): Invoked by `api_client.summarize_video()` (`POST /upload_stream`). Samples video keyframes and extracts audio tracks, polling `GET /status/{task_id}` for completion.
- **[Translator & Rephraser](file:///d:/projects/new%20EDMS%20py/Translator%20Rephraser%20py)** (`TRANSLATOR_REPHRASER_API_URL`): Invoked by `api_client.translate_text()` (`POST /generate`) on Port 5005 (or 5010). Employs GovAI `qwen3-235b-fp16`. Normalizes Arabic text by strictly stripping Tashkeel diacritics (`[\u064B-\u065F\u0670]`), translates extracted English vision/video tags into Arabic, shortens verbose image captions (> 300 chars), and tokenizes video audio transcripts. Stores bilingual keyword pairs in Oracle `KEYWORD` (`KEYWORD_ID` and `DESCRIPTION`) and `LKP_DOCUMENT_TAGS` to power cross-lingual search.

---

### 3.4 Enterprise Automation & Task Jobs

A fleet of autonomous Windows scheduled tasks support database and server health:

1. **[EDMS Crawler Task](file:///d:/projects/new%20EDMS%20py/Task%20Jobs/EDMS%20Crawler%20Task%20py)**: Compiled executable (`EDMSCrawlerTask.exe`) running under `NT AUTHORITY\SYSTEM` every 30 minutes. Authenticates and triggers `POST /process-batch`, acting as a pacing buffer to protect GPUs and GovAI LLM endpoints from concurrency spikes.
2. **[EDMS Auto Check-In Job](file:///d:/projects/new%20EDMS%20py/Task%20Jobs/EDMS%20Auto%20Checkin%20Job)**: Standalone utility (`edms_auto_checkin.exe`) that detects locked documents in Oracle `CHECKOUT` (`CHECKIN_DATE IS NULL`) and releases them via eDOCS WSDL (`%STATUS = %UNLOCK`).
3. **[EDMS Inactive Users Job](file:///d:/projects/new%20EDMS%20py/Task%20Jobs/EDMS%20Inactive%20Users%20Job)**: Nightly governance utility (`disable_inactive_users.exe`) identifying accounts inactive for > 90 days and setting `DISABLED = 'Y'` in `PEOPLE`, while strictly safeguarding service accounts in `LKP_SERVICE_USERS`.
4. **[EDMS Restart DM Server Job](file:///d:/projects/new%20EDMS%20py/Task%20Jobs/EDMS%20Restart%20DM%20Server%20Job)**: Server maintenance utility (`restart_edocs_dm.exe`) that stops and restarts eDOCS DM Windows services in dependency order, recycles the IIS application pool, and verifies post-restart health via WSDL HTTP probes.
5. **[Oracle Session Monitor](file:///d:/projects/new%20EDMS%20py/Oracle%20Session%20Monitor)**: Maintenance reaper (`edms_oracle_kill_sessions.exe`) executing on weekends to terminate orphaned Oracle database sessions alive for > 180 minutes, complementing the API's live `/api/admin/oracle-sessions` control routes.

---

### 3.5 Enterprise Storage & Core Platforms

- **OpenText eDOCS DM**: Enterprise document management system accessed via SOAP 1.1 / WSDL. Stores physical binaries, folder security trees, and trustee access control lists.
- **Oracle Database 19c Enterprise**: Master relational repository storing profiles, tags, events, favorites, user settings, quotas, and organizational structures.
- **ChromaDB**: On-disk vector database storing 4096-dimensional dense vector embeddings in collection `edms_documents` with Euclidean/Cosine indexing.
- **GovAI Sovereign AI Cloud**: Sovereign UAE AI platform (`https://llmapi.govai.ae`) hosting foundation models:
  - `qwen3-235b-fp16`: Natural language query understanding, translation, concept tokenization.
  - `qwen3-vl-30b`: Multimodal visual comprehension for OCR, image captioning, and video keyframes.
  - `qwen3-embedding-8b`: 4096-dimensional text vector embeddings.
  - `whisper_asr`: Audio speech-to-text transcription.
- **Enterprise AI Center (Dataiku DSS)**: Dedicated on-premise deep learning cluster (`rtauevaidss1.rtatestdom.local:13000`) hosting neural network weights for RetinaFace and VGG-Face.

---

## 4. Technology Stack

| Layer | Component | Description |
|---|---|---|
| **API Framework** | FastAPI (ASGI) | Async routing, dependency injection (`Depends`), Pydantic v2 schemas |
| **ASGI Server** | Uvicorn | High-concurrency async server, multi-worker process manager |
| **Relational Database** | Oracle Database 19c | Thin driver via `oracledb`, connection pool, parameterized SQL |
| **DMS Integration** | OpenText eDOCS DM | SOAP/WSDL via `zeep` with custom binary buffer decompression |
| **Vector Store** | ChromaDB | Persistent local embedded vector database (4096-dim space) |
| **Queue Database** | SQLite 3 | WAL mode, busy timeout, multi-worker leader-election lease support |
| **Document Processing** | PyMuPDF (fitz), Pillow | PDF parsing, image normalization, text overlay watermarking |
| **Video Processing** | MoviePy, FFmpeg | Hardware-accelerated video watermarking (NVIDIA / AMD / Intel QSV) |
| **Security & Auth** | Starlette SessionMiddleware | HTTP-only cookie sessions (60-day TTL, `SameSite=Lax`), MD5 Base64 hashing |
| **Rate Limiting** | SlowAPI | Redis-free in-memory rate limiting per endpoint per IP |
| **Email Delivery** | Python smtplib | SMTP notification delivery for OTP share links with HTML branding |
| **Process Hosting** | Microsoft IIS | Windows Server IIS reverse-proxying via `HttpPlatformHandler` |

---

## 5. Codebase Structure & Directory Layout

```
EDMS API/
├── app.py                         # FastAPI entry point, middleware stack, lifespan management
├── worker.py                      # Dedicated background processing worker (standalone daemon)
├── db_connector.py                # Central re-export hub for all database operations
├── api_client.py                  # HTTP client interfacing with downstream AI microservices
├── vector_client.py               # ChromaDB client & ExternalEmbeddingFunction wrapper
├── backfill_vectors.py            # Standalone batch indexing utility for historical documents
│
├── routes/                        # Domain-organized API route handlers
│   ├── auth.py                    # Login/logout, session user, language/theme, groups, trustees
│   ├── documents.py               # Document fetch, upload, watermarked download, batch enqueue
│   ├── media.py                   # Raw image/PDF/video streaming, thumbnails, cache clearing
│   ├── folders.py                 # Folder tree CRUD, item moves, recursive ZIP download
│   ├── tags.py                    # Tag/keyword CRUD, batch tag fetching, person additions
│   ├── events.py                  # Event CRUD, document-event associations, timeline journeys
│   ├── favorites.py               # Document favorites toggle and listing
│   ├── memories.py                # "On This Day" historical anniversary queries
│   ├── sharing.py                 # Shareable links, SMTP OTP verification, shared streaming
│   ├── admin.py                   # User management, queue pausing/resuming, tab permissions
│   ├── edocs_admin.py             # Server-side OpenText eDOCS cache clearing
│   ├── ems_admin.py               # EMS organizational admin: agencies, departments, sections
│   ├── edms_people.py             # Full CRUD for PEOPLE table, HR employee provisioning
│   └── profilesearch.py          # Dynamic multi-criteria profile search across Oracle tables
│
├── database/                      # Data access layer (Oracle DB & SQLite)
│   ├── connection.py              # Oracle connection pooling, async wrappers, blocklist loading
│   ├── documents.py               # Complex filtered document fetch, hybrid search, RRF ranking
│   ├── media.py                   # DMS media streaming, thumbnail generators, app-bucket caching
│   ├── folders.py                 # Folder hierarchy queries, breadcrumb generators
│   ├── tags.py                    # Tag/person CRUD, batch queries, English↔Arabic persistence
│   ├── events.py                  # Event CRUD and timeline journey data aggregation
│   ├── favorites.py               # Per-user favorites queries
│   ├── memories.py                # "On This Day" date matching logic
│   ├── users.py                   # User profile data, theme/language preferences
│   ├── user_data.py               # User storage quota deduction and restoration
│   ├── groups.py                  # DMS group queries and membership validation
│   ├── sharing.py                 # Share link storage, OTP verification, access logging
│   ├── admin.py                   # Admin operations: add/update/delete EDMS users
│   ├── ems_admin.py               # EMS organizational hierarchy data management
│   ├── edms_people.py             # PEOPLE table, PEOPLEGROUPS, NETWORK_ALIASES, HR linkage
│   ├── tab_permissions.py         # Tab visibility permissions per user
│   ├── profilesearch.py          # Dynamic SQL builder for multi-criteria form searches
│   └── processing_queue.sqlite3   # Persistent SQLite queue database (WAL mode)
│
├── schemas/                       # Pydantic v2 validation models & request/response schemas
│   ├── auth.py                    # Login credentials, user preferences, trustee schemas
│   ├── documents.py               # Document upload, metadata update, security schemas
│   ├── folders.py                 # Folder creation, rename, move schemas
│   ├── sharing.py                 # Share links, OTP verification, folder content schemas
│   ├── tags.py                    # Tag creation, person registration schemas
│   ├── events.py                  # Event creation and linking schemas
│   └── edocs_cache.py             # eDOCS cache clearing request/response schemas
│
├── services/                      # Core business logic & processing engines
│   ├── processor.py               # AI orchestration engine (caption, OCR, face, video, vector)
│   ├── processing_queue.py        # SQLite queue manager, worker loop, mode audit trail
│   └── edocs_cache.py             # Passive OpenText eDOCS DM server cache purger
│
├── utils/                         # Shared utilities
│   ├── common.py                  # Auth dependencies (`get_current_user`), SMTP email sender
│   ├── watermark.py               # Digital watermarking (FFmpeg HW acceleration + PyMuPDF)
│   ├── sanitize.py                # Input text and filename sanitization (stored XSS defense)
│   ├── cache_eviction.py          # LRU video cache cleanup background task
│   ├── log_cleanup.py             # 14-day rolling IIS/application log file purger
│   └── ttl_cache.py               # Thread-safe in-memory cache for search metadata
│
├── wsdl_client/                   # OpenText eDOCS SOAP / WSDL integration adapter
│   ├── config.py                  # WSDL endpoints and system credentials
│   ├── base.py                    # Zeep client factory and error wrappers
│   ├── auth.py                    # User authentication and cached system login (30-min TTL)
│   ├── documents.py               # DMS binary upload, download, streaming, security ACL
│   ├── folders.py                 # DMS folder creation, rename, move, recursive delete
│   ├── users.py                   # DMS group queries, membership queries
│   └── utils.py                   # Binary buffer parser (6-strategy zlib decompression, UTF-16LE)
│
├── diagrams/                      # System architecture diagrams
│   ├── smart_edms_architecture.jpg # Rendered architecture infographic (Ingestion & Search)
│   ├── render_diagram.html        # HTML/SVG source template for architecture diagram
│   ├── EDMS_Architecture_High_Level.pdf
│   └── EDMS_Architecture_Technical_Details.pdf
│
├── chroma_db/                     # Persistent on-disk ChromaDB vector store
├── thumbnail_cache/               # Persistent thumbnail cache directory
├── temp_thumbnail_cache/          # Ephemeral thumbnail cache (24h TTL)
├── video_cache/                   # Cached video files for HTTP Range request streaming
├── download_cache/                # Download staging area for watermark application
├── static/                        # Static assets (company branding, email logo assets)
├── blocklist.json                 # Stop words, profanity, and meaningless tag filtering rules
├── requirements.txt               # Pinned Python package dependencies
├── .env                           # Active environment variables and service URLs
├── run_api.bat                    # Production Uvicorn launcher (4 workers)
├── setup.bat                      # Automated offline virtualenv installer
├── download_packages.bat          # Online wheel pre-fetcher for air-gapped deployment
├── install_packages.bat           # Offline wheel installer
├── create_archive.bat             # Release zip packaging utility
├── web.config                     # Primary Windows IIS HttpPlatformHandler configuration
└── web2.config                    # Alternate production IIS configuration with tuning
```

---

## 6. API Endpoints Reference

All routes require active session authentication via the `get_current_user` FastAPI dependency, unless explicitly noted.

### 6.1 Authentication & User Sessions

| Method | Endpoint | Description | Rate Limit |
|---|---|---|---|
| `POST` | `/api/auth/login` | Authenticates user against eDOCS SOAP DMS. Creates server-side signed cookie session. | 5 / min |
| `POST` | `/api/auth/logout` | Terminates user session and invalidates cookie. | — |
| `GET` | `/api/auth/user` | Returns current user details, security level, groups, and tab permissions. | — |
| `PUT` | `/api/user/language` | Updates UI language preference (`en` / `ar`) in Oracle DB. | — |
| `PUT` | `/api/user/theme` | Updates UI theme preference (`light` / `dark`) in Oracle DB. | — |
| `GET` | `/api/groups` | Lists eDOCS DMS groups (Admins view all; standard users view assigned). | — |
| `GET` | `/api/groups/{group_id}/members` | Lists members belonging to a specific DMS group. | — |
| `GET` | `/api/groups/search_members` | Searches for specific users within a DMS group. | — |
| `GET` | `/api/document/{doc_id}/trustees` | Returns access control list (trustees) assigned to a document. | — |

---

### 6.2 Documents & Ingestion

| Method | Endpoint | Description | Rate Limit |
|---|---|---|---|
| `GET` | `/api/documents` | Hybrid search document fetch. Supports text search, vector search, date filters, person tags, media type, year, sort, and memories mode. | — |
| `POST` | `/api/upload_document` | Uploads document to eDOCS DMS, extracts EXIF date, inherits parent security, deducts user quota, records row in Oracle DB. | 10 / min |
| `POST` | `/api/process_uploaded_documents` | Enqueues newly uploaded document numbers into the SQLite processing queue. | — |
| `POST` | `/process-batch` | **Master Crawler Trigger**: Queries Oracle for unprocessed documents (`STATUS = 0`) and enqueues them for background AI processing. | 3 / min |
| `GET` | `/api/document/{docnumber}` | Streams raw binary document directly from eDOCS DMS for inline preview. | — |
| `PUT` | `/api/update_metadata` | Updates a document's abstract, title, and/or date taken in Oracle DB. | — |
| `GET` | `/api/download_watermarked/{doc_id}` | Downloads document with a personalized watermark (user system ID + timestamp). Watermarks images (Pillow), PDFs (PyMuPDF), and videos (FFmpeg). | — |
| `POST` | `/api/update_abstract` | Updates a document's abstract with detected VIP names. | — |
| `PUT` | `/api/document/{doc_id}/event` | Associates a document with an organizational event. | — |
| `GET` | `/api/document/{doc_id}/event` | Retrieves the event linked to a document. | — |
| `POST` | `/api/document/{doc_id}/security` | Configures document trustees and access permissions in eDOCS. | — |

---

### 6.3 Media Streaming & Caching

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/image/{doc_id}` | Streams an image file from eDOCS with image MIME headers. |
| `GET` | `/api/pdf/{doc_id}` | Streams a PDF document from eDOCS (configured with CSP headers for iframe viewing). |
| `GET` | `/api/video/{doc_id}` | Streams video with disk caching and HTTP `Range` request support for seeking. |
| `GET` | `/api/temp_thumbnail/{doc_id}` | Generates and serves an ephemeral thumbnail (24-hour TTL). |
| `GET` | `/cache/{filename}` | Serves cached thumbnails from persistent disk storage. |
| `POST` | `/api/clear_cache` | Purges all thumbnail, temporary, and video caches (Editor/Admin required). |
| `GET` | `/api/media_counts` | Returns aggregate counts of images, videos, and files filtered by app source. |

---

### 6.4 Folders & Hierarchy Navigation

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/folders` | Lists folder contents (subfolders + documents) with scope and media filters. |
| `POST` | `/api/folders` | Creates a new folder in eDOCS DMS, inheriting parent security. |
| `PUT` | `/api/folders/{folder_id}` | Renames a folder in eDOCS DMS. |
| `DELETE` | `/api/folders/{folder_id}` | Deletes a folder. If child items exist, performs recursive force-deletion and restores user quota. |
| `POST` | `/api/folders/move-items` | Moves multiple documents and subfolders to a new parent folder. |
| `GET` | `/api/folders/{folder_id}/download-zip` | Generates a ZIP archive containing all documents in a folder (enforces 300 MB ceiling). |

---

### 6.5 Tags, Persons & VIP Recognition

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/tags` | Retrieves all active tags respecting user security level and app source. |
| `GET` | `/api/tags/{doc_id}` | Fetches tags assigned to a specific document. |
| `POST` | `/api/tags/batch` | Batch-fetches tags for up to 50 documents simultaneously (eliminates N+1 queries). |
| `POST` | `/api/tags/{doc_id}` | Assigns a new tag to a document (auto-translates between English and Arabic). |
| `DELETE` | `/api/tags/{doc_id}/{tag}` | Removes a tag association from a document. |
| `POST` | `/api/tags/shortlist` | Toggles a tag's shortlisted status for quick UI access. |
| `POST` | `/api/add_person` | Registers a new VIP in `LKP_PERSON` (auto-translates name via GovAI). |
| `GET` | `/api/persons` | Searches registered VIP persons with pagination. |
| `POST` | `/api/processing_status` | Returns whether documents are currently undergoing AI processing. |

---

### 6.6 Events & Timeline Journeys

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/events` | Lists organizational events with pagination, search, and dropdown modes. |
| `POST` | `/api/events` | Creates a new organizational event. |
| `GET` | `/api/events/{event_id}/documents` | Retrieves all documents tagged under an event with pagination. |
| `GET` | `/api/journey` | Aggregates chronological journey milestones across documented events. |

---

### 6.7 Favorites & Memories ("On This Day")

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/favorites/{doc_id}` | Adds a document to the current user's favorites list (`LKP_FAVORITES_DOC`). |
| `DELETE` | `/api/favorites/{doc_id}` | Removes a document from favorites. |
| `GET` | `/api/favorites` | Returns the authenticated user's favorite documents with pagination. |
| `GET` | `/api/memories` | Returns "On This Day" documents—photographs and media taken on the same month and day in previous years. |

---

### 6.8 Secure Sharing & OTP Flow

| Method | Endpoint | Description | Rate Limit |
|---|---|---|---|
| `POST` | `/api/share/generate` | Creates secure share link. Supports **Open mode** (any `@rta.ae` email) and **Restricted mode** (specific recipients). | 100 / min |
| `GET` | `/api/share/info/{token}` | Public route returning link metadata (restricted vs. open, expiry) without authentication. | — |
| `POST` | `/api/share/request-access/{token}` | Validates recipient email, generates 6-digit OTP, stores in DB, sends via SMTP. | 5 / min |
| `POST` | `/api/share/verify-access/{token}` | Validates 6-digit OTP code, issues temporary access token, logs audit trail. | — |
| `GET` | `/api/share/folder-contents/{token}`| Retrieves shared folder tree, verifying requested path is within shared root. | — |
| `GET` | `/api/share/stream/{token}` | Streams shared document for inline viewing (no watermark). | — |
| `GET` | `/api/share/download/{token}` | Downloads file watermarked with the recipient's verified email + timestamp. | 10 / min |

---

### 6.9 System Administration & Queue Management

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/admin/check-access` | Verifies whether the authenticated user is on the admin allowlist. |
| `GET` | `/api/admin/users` | Lists EDMS users with pagination and search. |
| `POST` | `/api/admin/users` | Registers a new user into Smart EDMS from the `PEOPLE` table. |
| `PUT` | `/api/admin/users/{id}` | Updates user quota, security level (Admin/Editor/Viewer), language, and theme. |
| `DELETE` | `/api/admin/users/{id}` | Removes a user from the Smart EDMS platform. |
| `GET` | `/api/admin/processing-queue/status`| Returns queue metrics: pending, in-progress, completed, failed, and worker mode. |
| `POST` | `/api/admin/processing-queue/worker/pause` | Pauses background queue workers. |
| `POST` | `/api/admin/processing-queue/worker/resume`| Resumes background queue workers. |
| `POST` | `/api/admin/processing-queue/worker/drain` | Drains the queue: finishes running jobs, then transitions to paused. |
| `POST` | `/api/admin/processing-queue/retry-failed` | Re-queues all failed processing jobs and resets attempt counters. |
| `POST` | `/api/admin/processing-queue/retry-selected`| Retries specific failed jobs by document numbers. |
| `DELETE` | `/api/admin/processing-queue/completed` | Purges completed queue records older than N hours. |
| `GET` | `/api/admin/tab-permissions/{user_id}` | Retrieves tab visibility permissions for a specific user. |
| `PUT` | `/api/admin/tab-permissions` | Creates or updates a tab permission entry. |
| `POST` | `/api/admin/tab-permissions/init/{user_id}`| Initializes default tab permissions for a newly provisioned user. |

---

### 6.10 eDOCS Server Cache Administration

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/admin/edocs/clear-cache` | Triggers a non-destructive, passive server-side clear of OpenText eDOCS local cache zones (`Default`, `metadata`, `index`, `echo`, `temp`, `document`) under `%APPDATA%/OpenText/DM/Cache/`. Skips locked files. Returns `202 Accepted` immediately. |

---

### 6.11 EMS Organizational Administration

Manages organizational structures: **Agencies ➔ Departments ➔ Sections**.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/ems-admin/check-access` | Validates EMS admin access (requires `EMS_ADMIN` group or Editor+). |
| `GET` | `/api/departments/agencies` | Lists all active government agencies. |
| `GET` | `/api/sections` | Lists company sections with pagination and agency filters. |
| `POST` | `/api/sections/add` | Registers a new company section. |
| `PUT` | `/api/sections/update` | Updates section properties. |
| `GET` | `/api/departments` | Lists departments with pagination and agency filtering. |
| `POST` | `/api/departments/add` | Registers a new department. |
| `PUT` | `/api/departments/update` | Updates department metadata. |
| `GET` | `/api/ems_sections` | Lists EMS organizational sections. |
| `POST` | `/api/ems_sections/add` | Registers an EMS section. |
| `PUT` | `/api/ems_sections/update` | Updates an EMS section. |

---

### 6.12 EDMS People Management & HR Provisioning

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/edms-people` | Lists users in the `PEOPLE` table with HR hierarchy associations. |
| `GET` | `/api/edms-people/hr-employees` | Searches HR employees (`LKP_HR_EMPLOYEES`) not yet provisioned in EDMS. |
| `GET` | `/api/edms-people/groups` | Lists all available DMS groups for user assignment. |
| `GET` | `/api/edms-people/{system_id}/details`| Returns detailed account info: groups, network aliases, and HR hierarchy. |
| `POST` | `/api/edms-people` | Provisions an EDMS user: creates `PEOPLE` record, assigns groups via `PEOPLEGROUPS`, creates network aliases, auto-assigns `DOCS_USERS`, and flags HR record with `IS_EDMS_USR = 'Y'`. |
| `PUT` | `/api/edms-people/{system_id}` | Updates existing user details, rebuilds group memberships, updates aliases. |
| `GET` | `/api/edms-people/export` | Exports all active EDMS users and organizational mappings to Excel (`.xlsx`). |

---

### 6.13 Dynamic Profile Search

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/profilesearch/scopes` | Returns available search scopes for the user (cached 5 min). Scope `0` represents Global cross-form search; specific Form IDs (e.g., 2572, 3799) target individual tables. |
| `GET` | `/api/profilesearch/types` | Returns search fields and data types configured for a scope. |
| `POST` | `/api/profilesearch/search` | Dynamic multi-criteria search: accepts up to 6 search criteria with `AND` logic, scope, date ranges, and pagination, dynamically building optimized SQL queries. |

---

### 6.14 Oracle Database Session Monitoring

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/admin/oracle-sessions` | Inspects active Oracle database sessions from `V$SESSION`, returning session IDs, usernames, machines, programs, logon times, and inactive durations. |
| `POST` | `/api/admin/oracle-sessions/kill` | Safely terminates an inactive or hanging Oracle session via `ALTER SYSTEM KILL SESSION 'sid,serial' IMMEDIATE`. Rejects system accounts. |

---

## 7. AI Document Processing Pipeline

The central processing engine resides in `services/processor.py` (`process_document()`). All synchronous HTTP client calls to downstream microservices are dispatched to a worker thread via `run_in_threadpool()` to prevent event loop blocking.

```
Document Upload / Queue Worker
       │
       ├─► Images ───► Image Captioning (5001) ─► Caption + Descriptive Tags (GovAI qwen3-vl-30b)
       │         ───► OCR Service (5004)        ─► Extracted Arabic/English Text (GovAI qwen3-vl-30b)
       │         ───► Face Recognition (5002)   ─► PIL Crops ─► Dataiku Model API (5009) ─► AI Center
       │
       ├─► Videos ───► Video Summarizer (5008)  ─► Strategic Keyframe Vision (GovAI qwen3-vl-30b)
       │                                        ─► Audio ASR Transcription (GovAI whisper_asr)
       │                                        ─► Face Crops ─► Face Recognition (5002) ─► Dataiku
       │
       └─► PDFs ─────► PyMuPDF Page Extraction  ─► OCR Service (5004) ─► Text Tokens
                              │
                              ▼
        Consolidate Abstract & Bilingual Translations (Translator Rephraser :5005 / :5010)
        (Translates tags EN➔AR without Tashkeel, shortens captions >300 ch, caps abstract <2000 ch)
                              │
                              ▼
            Update Oracle DB (STATUS = 3, Abstract, Tags, VIPs)
                              │
                              ▼
             ChromaDB Vector Indexing (Embedding Service :5003)
```

### Media-Specific Flows:

- **Images**:
  1. Calls Captioning Service to produce scene description (shortened via Translator Service if > 300 characters).
  2. Calls OCR Service to extract in-image text.
  3. Calls Face Recognition Service (`/api/analyze_image_stream`) to identify VIP dignitaries.
  4. Tags are translated to Arabic without Tashkeel via Translator Service (Port 5005) and committed as bilingual pairs (`KEYWORD_ID` + `DESCRIPTION`) to `KEYWORD` and `LKP_DOCUMENT_TAGS`.
  5. The consolidated abstract is indexed into ChromaDB.
- **Videos**:
  1. Streams video chunks to Video Context Summarizer (`/upload_stream`).
  2. Polls `/status/{task_id}` for completion.
  3. Extracted face thumbnails are sent to Face Recognition Service (`/api/recognize_faces`).
  4. Audio transcripts are tokenized for keywords.
  5. Findings are merged into the video abstract and tagged in Oracle DB.
- **PDFs**:
  1. Renders PDF pages to memory buffers via PyMuPDF.
  2. Dispatches pages to OCR Service for bilingual text extraction.
  3. Tokenizes and indexes text into Oracle DB and ChromaDB.

---

## 8. Background Processing Queue & Worker Process

Heavy AI processing is decoupled from web requests using a local SQLite job queue (`database/processing_queue.sqlite3`):

- **Database Engine**: SQLite running in **WAL (Write-Ahead Logging)** mode with a 5,000ms busy timeout.
- **Unique Constraint**: Enforces unique `docnumber` to prevent duplicate concurrent jobs.
- **Worker Concurrency**: Worker claims up to 3 jobs per cycle (`LIMIT 3`).
- **Task Timeout**: Guarded by `PROCESSING_TASK_TIMEOUT_SECONDS` (default: 300s / 5 minutes) to prevent hung jobs from blocking execution.
- **Automated Retries**: Transient failures are retried after a 20-second delay up to **3 maximum attempts**.
- **Worker Modes**:
  - `running`: Actively claiming and executing queued jobs.
  - `paused`: Suspends job claiming (default on initial server boot).
  - `draining`: Finishes running jobs, then transitions to paused.
- **Audit Logging**: All mode transitions are logged to table `worker_mode_audit` with timestamp, actor, and reason.

### Running the Worker Process:

In production, run `worker.py` as a supervised Windows Service:
```bash
venv\Scripts\python.exe worker.py
```

---

## 9. Vector Search Engine (ChromaDB)

- **Vector Collection**: `edms_documents` stored on disk at `CHROMA_DB_PATH`.
- **Vector Dimension**: **4096 dimensions** produced by GovAI `qwen3-embedding-8b`.
- **Custom Embedding Function**: `vector_client.py` implements `ExternalEmbeddingFunction`, bridging ChromaDB with the Embedding Service microservice.
- **Distance Cutoff**: Configurable via `VECTOR_DISTANCE_THRESHOLD` (default: 1.3).
- **Historical Backfill**: Historical records in Oracle DB can be indexed in bulk using `backfill_vectors.py`:
  ```bash
  python backfill_vectors.py
  ```

---

## 10. OpenText eDOCS WSDL / SOAP Client

The `wsdl_client/` package implements an enterprise adapter over the OpenText eDOCS DM SOAP API:

- **Authentication Cache**: System-level tokens (`dms_system_login`) are cached for 30 minutes with automatic renewal.
- **Binary Buffer Parser (`wsdl_client/utils.py`)**:
  OpenText eDOCS returns data in proprietary binary buffers (base64-encoded, optionally zlib-compressed, with length-prefixed UTF-16LE strings). The custom parser implements **6 sequential decompression strategies**:
  1. Stripping DMS `0xFFFFFFFF` magic markers.
  2. Standard zlib header decoding.
  3. Raw DEFLATE without headers (`wbits=-15`).
  4. Header offset probing (offsets 2, 4, 6, 8).
  5. Heuristic column detection (identifying `USER_ID` vs. `FULL_NAME` based on whitespace and casing).
  6. Folder tree item decoding (extracting nodes, folders, and media types).

---

## 11. Database Layer & Oracle Connection Management

The database layer (`database/`) manages all Oracle interaction via the modern `oracledb` thin client driver:

- **Connection Pool**: Initialized in `database/connection.py` with min/max pool boundaries and automatic session recycling.
- **Index Bootstrapping**: At application startup, the API automatically executes a best-effort index creation script (30-second budget) ensuring indexes exist on `PROFILE.RTADOCDATE`, `PROFILE.ABSTRACT`, `TAGGING_QUEUE`, and `LKP_DOCUMENT_TAGS`.
- **Blocklist Filtering**: Loads `blocklist.json` at startup, filtering stop words, profanity, and meaningless single characters from AI tags.
- **Dynamic SQL Query Builder (`database/profilesearch.py`)**: Assembles parameterized SQL statements based on configured form schema definitions, supporting starts-with, contains, and exact matching without SQL injection risk.

---

## 12. Utilities & Media Processing

- **Digital Watermarking (`utils/watermark.py`)**: Applies user identification and timestamp watermarks to downloaded media:
  - **Images**: Pillow text overlay (semi-transparent, bottom-right).
  - **PDFs**: PyMuPDF (`fitz`) text insertion on all pages.
  - **Videos**: Hardware-accelerated FFmpeg pipeline (auto-detects NVIDIA `h264_nvenc`, AMD `h264_amf`, or Intel `h264_qsv`, with MoviePy CPU fallback). Watermark displays at start, end, and randomized intervals.
- **Input Sanitization (`utils/sanitize.py`)**: Strips malicious HTML/JavaScript tags and control characters to prevent stored XSS attacks.
- **Cache Eviction (`utils/cache_eviction.py`)**: Scheduled background task running every 6 hours to purge video cache files older than 24 hours (LRU policy).
- **Log Purging (`utils/log_cleanup.py`)**: Daily background task purging application and IIS log files older than 14 days.

---

## 13. Middleware Stack & Security Headers

HTTP requests pass through the following middleware stack (in execution order):

1. **Security Headers Middleware**: Enforces `Content-Security-Policy`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: strict-origin-when-cross-origin`, and `X-Frame-Options: DENY` (except `/api/pdf/` routes configured for iframe embedding).
2. **CORS Middleware**: Configured via `FRONTEND_URL` environment variable; allows credentials for cookie-based session persistence.
3. **Starlette SessionMiddleware**: Cookie-based encrypted session storage (60-day expiry, `SameSite=Lax`).
4. **Request Metrics Middleware**: Attaches `X-Request-ID` tracing headers, measures request duration, and emits structured performance telemetry.

---

## 14. Security & Access Control Model

### Three-Tier Authorization:

| Level | Rank | Permissions |
|---|:---:|---|
| **Admin** | `9` | Full administrative access: user management, queue control, eDOCS cache clearing, tab permissions, system exports. |
| **Editor** | `5` | Document upload, metadata updates, tag management, folder creation/movement, cache clearing. |
| **Viewer** | `0` | Read-only access: view documents, stream media, search, and download watermarked copies. |

- **Elevated Groups**: Users belonging to DMS group `EMS_ADMIN` receive access to organizational hierarchy and EDMS People management. Users in `DOCS_SUPERVISORS` receive global document visibility across private folders.
- **Document-Level Security**: Enforced via eDOCS trustees (ACL) inherited from parent folders or configured per document.
- **Rate Limiting**: Enforced via SlowAPI across sensitive endpoints:
  - Login: 5 requests / min
  - Document Upload: 10 requests / min
  - Batch Process Trigger: 3 requests / min
  - OTP Access Request: 5 requests / min
  - Share Download: 10 requests / min

---

## 15. Multi-Node & Load-Balanced Deployment

When running behind a load balancer with multiple API server nodes, all stateful file-based resources must point to a shared network mount (NAS / SMB share):

```ini
# Shared Network Mount Paths
CHROMA_DB_PATH=\\nas-server\edms-shared\chroma_db
PROCESSING_QUEUE_DB_PATH=\\nas-server\edms-shared\processing_queue.sqlite3
IDEMPOTENCY_DB_PATH=\\nas-server\edms-shared\idempotency.sqlite3
THUMBNAIL_CACHE_DIR=\\nas-server\edms-shared\thumbnail_cache
VIDEO_CACHE_DIR=\\nas-server\edms-shared\video_cache
TEMP_THUMBNAIL_CACHE_DIR=\\nas-server\edms-shared\temp_thumbnail_cache
DOWNLOAD_CACHE_DIR=\\nas-server\edms-shared\download_cache
```

### Leader Election for Background Tasks:

To prevent duplicate queue processing and cache cleanup runs across cluster nodes:
- `WORKER_ROLE=auto`: Nodes negotiate leadership via a shared lease file (`worker_lease.json`).
- `WORKER_LEASE_TTL=30`: Lease expires after 30 seconds of inactivity, triggering automated failover.
- `WORKER_LEASE_RENEW_INTERVAL=10`: Active leader renews its lease every 10 seconds.
- Session consistency is guaranteed by sharing an identical `FLASK_SECRET_KEY` across all nodes.

---

## 16. Offline Setup & Air-Gapped Installation

The repository supports 100% offline installation in secure, air-gapped environments:

```
[INTERNET-CONNECTED MACHINE]
   │
   ├── 1. Run download_packages.bat  ──► Downloads wheels for Python 3.12 (win_amd64) into packages/
   ├── 2. Run create_archive.bat     ──► Bundles source + wheels into ..\EDMS_API_YYYY-MM-DD.zip
   │
[SECURE FILE TRANSFER (SFTP / USB)]
   │
   ▼
[AIR-GAPPED PRODUCTION SERVER]
   │
   ├── 3. Extract the ZIP archive
   ├── 4. Run setup.bat              ──► Creates venv and executes:
   │                                     pip install --no-index --find-links=packages -r requirements.txt
   ├── 5. Configure .env with database credentials and microservice URLs
   └── 6. Launch via run_api.bat or attach to IIS
```

---

## 17. Windows IIS Production Deployment (`web2.config`)

In Windows Server IIS environments, requests are handled via the `HttpPlatformHandler` module:

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <system.webServer>
    <handlers>
      <add name="httpPlatformHandler" path="*" verb="*" modules="httpPlatformHandler" resourceType="Unspecified" />
    </handlers>
    <httpPlatform processPath="venv\Scripts\python.exe"
                  arguments="-m uvicorn app:app --host 0.0.0.0 --port %HTTP_PLATFORM_PORT% --workers 4"
                  startupTimeLimit="300"
                  requestTimeout="00:20:00"
                  stdoutLogEnabled="true"
                  stdoutLogFile=".\logs\python-stdout.log">
      <environmentVariables>
        <environmentVariable name="PYTHONPATH" value="." />
      </environmentVariables>
    </httpPlatform>
    <security>
      <requestFiltering>
        <requestLimits maxAllowedContentLength="1073741824" />
      </requestFiltering>
    </security>
  </system.webServer>
</configuration>
```

- **Port Assignment**: Dynamic internal binding via `%HTTP_PLATFORM_PORT%`, reverse-proxied to public port `5000`.
- **Large Upload Ceiling**: `maxAllowedContentLength="1073741824"` permits uploads up to 1 GB.
- **Process Supervision**: Spawns 4 concurrent Uvicorn worker processes with a 20-minute request timeout.

---

## 18. Environment Variables Reference (`.env`)

```ini
# ==============================================================================
# Oracle Enterprise Database
# ==============================================================================
DB_HOST=10.0.0.50
DB_PORT=1521
DB_SERVICE_NAME=edmsprod
DB_USERNAME=docsadmin
DB_PASSWORD=YourSecurePassword

# ==============================================================================
# OpenText eDOCS DM Server (SOAP/WSDL)
# ==============================================================================
WSDL_URL=http://edocs-server:8080/DOCS/DOCSWS.asmx?WSDL
DMS_USER=DOCSADM
DMS_PASSWORD=YourDmsPassword

# ==============================================================================
# Downstream AI Microservices
# ==============================================================================
CAPTIONING_API_URL=http://localhost:5001
FACE_API_URL=http://localhost:5002
EMBEDDING_API_URL=http://localhost:5003
OCR_API_URL=http://localhost:5004
SMART_SEARCH_API_URL=http://localhost:5006
VIDEO_SUMMARIZER_API_URL=http://localhost:5008
TRANSLATOR_REPHRASER_API_URL=http://localhost:5005  # Port 5005 (or 5010 fallback)

# ==============================================================================
# Vector Store & Shared Paths
# ==============================================================================
CHROMA_DB_PATH=./chroma_db
CHROMA_COLLECTION_NAME=edms_documents
VECTOR_DISTANCE_THRESHOLD=1.3

# ==============================================================================
# Application & Session Security
# ==============================================================================
FLASK_SECRET_KEY=cryptographically-random-secret-key-must-match-across-nodes
FRONTEND_URL=http://localhost:3000,http://portal.rta.ae
API_BASE_URL=http://api.edms.rta.ae

# ==============================================================================
# SMTP Mail Configuration (OTP Sharing)
# ==============================================================================
SMTP_SERVER=mail.rta.ae
SMTP_PORT=587
SMTP_USER=edms-notifications@rta.ae
SMTP_PASSWORD=YourSmtpPassword
SMTP_SENDER_EMAIL=edms-notifications@rta.ae
SMTP_SENDER_NAME="Smart EDMS"
COMPANY_NAME="Roads and Transport Authority"
SUPPORT_EMAIL=edms-support@rta.ae
```

---

## 19. Troubleshooting & Operational Runbook

| Symptom / Error | Root Cause | Remediation Procedure |
|:---|:---|:---|
| **`HTTP 502 Bad Gateway` on Search** | Smart Search (5006) or Embedding Service (5003) is unreachable. | The API automatically falls back to Oracle SQL `LIKE` search. Verify that microservices are online: `curl http://localhost:5006/health` and `curl http://localhost:5003/health`. |
| **`Dimension mismatch detected` in ChromaDB** | Collection was created with 384-dim embeddings instead of 4096-dim. | Rename existing `chroma_db` folder to `chroma_db_backup`. Restart the API to initialize the 4096-dim collection, then execute `python backfill_vectors.py`. |
| **`DatabaseConnectionError: DPI-1047`** | Oracle Instant Client library is missing or incompatible. | Ensure `python-oracledb` is running in thin mode (default), or install Oracle Instant Client 19c+ for thick mode. |
| **`SOAP Error: LoginSvr5 Failed`** | eDOCS DM service account password changed or DMS monitor service is stopped. | Run `restart_edocs_dm.exe --check-only` from `..\Task Jobs\EDMS Restart DM Server Job` to probe DM service availability. |
| **`HTTP 429 Too Many Requests` on `/process-batch`** | The batch crawler triggered more than 3 times in 1 minute. | Verify the Windows Task Scheduler cadence in `EDMS Crawler Task` is set to 30 minutes, not 10 seconds. |
| **Queue jobs stuck in `in_progress`** | Process was forcibly killed midway through an AI task. | Open the Admin Panel (`/api/admin/processing-queue/retry-failed`) or trigger `POST /api/admin/processing-queue/retry-failed` to reset pending attempt counts. |
| **`Access is denied (0x5)` in IIS** | Application Pool identity lacks file permissions. | Grant `IIS AppPool\<SiteName>` Read, Write, and Execute permissions on `thumbnail_cache/`, `video_cache/`, `download_cache/`, and `database/`. |
