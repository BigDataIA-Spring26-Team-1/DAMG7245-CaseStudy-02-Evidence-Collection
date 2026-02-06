# Case Study 2 — Evidence Collection for Organizational AI Readiness

Course: DAMG 7245 – Big Data Systems & Intelligence Analytics  
Term: Spring 2026  
Instructor: Sri Krishnamurthy  
Program: MS in Information Systems, Northeastern University  

---

## Team Members

Ayush Patil,
Piyush Kunjilwar,
Raghavendra Prasath Sridhar

---

## Overview

Case Study 2 (CS2) focuses on **evidence collection** for assessing Organizational AI Readiness under the **PE Org-AI-R framework**. The primary objective of this case study is to design and implement a **production-grade data ingestion and processing pipeline** that collects, processes, and stores **verifiable external evidence** relevant to organizational AI capabilities.

---

## Problem Statement

In enterprise and private equity contexts, AI readiness assessments must be:
- Evidence-backed
- Auditable
- Reproducible
- Defensible for investment and governance decisions

Manual research, subjective judgment, or anecdotal claims are insufficient. CS2 addresses this gap by building a systematic pipeline that transforms **raw, unstructured external documents** into **structured, traceable, and analytics-ready evidence**.

---

## Objectives

- Design a reproducible evidence collection pipeline  
- Ingest external document-based signals related to AI readiness  
- Parse and structure unstructured SEC filings  
- Implement deduplication to prevent redundant processing  
- Preserve traceability from insights back to source documents  
- Prepare semantically meaningful, token-bounded text chunks for LLM usage  

---

## Evidence Sources

### SEC Filings (Primary Evidence)

Public SEC filings are used as authoritative, legally disclosed sources of organizational information:

- **10-K**  
  Annual reports containing business strategy, AI disclosures, risk factors, and R&D investments  

- **10-Q**  
  Quarterly updates tracking progress on previously disclosed initiatives  

- **8-K**  
  Event-driven filings capturing material changes such as AI leadership hires, acquisitions, and partnerships  

- **DEF-14A**  
  Proxy statements providing governance, board expertise, and oversight signals  

### Document Formats

- **PDF filings**  
  Official documents containing structured financial tables and detailed disclosures  

- **HTML filings**  
  Faster parsing with better semantic structure for narrative sections  

---

## Pipeline Architecture

SEC EDGAR  
↓  
Rate-Limited Downloader  
↓  
PDF and HTML Parsing  
↓  
Semantic Chunking (500–1000 tokens)  
↓  
Content Hashing and Deduplication  
↓  
Document Registry  
↓  
Structured Storage (Snowflake or filesystem)  

Each stage is modular, testable, and designed using production-oriented engineering principles.

---

## Repository Structure

cs2-evidence-collection/
├── src/
│   ├── ingestion/
│   │   ├── sec_downloader.py
│   │   ├── rate_limiter.py
│   ├── parsing/
│   │   ├── pdf_parser.py
│   │   ├── html_parser.py
│   ├── chunking/
│   │   ├── semantic_chunker.py
│   ├── registry/
│   │   ├── document_registry.py
├── data/
│   ├── raw/
│   ├── processed/
├── tests/
│   ├── unit/
│   ├── integration/
├── docs/
│   ├── architecture.md
├── pyproject.toml
├── poetry.lock
├── README.md

---

## Key Design Decisions

### Rate Limiting

SEC EDGAR enforces a strict limit of 10 requests per second. The ingestion layer explicitly respects this constraint to ensure compliant access and avoid IP bans.

### Semantic Chunking

Documents are split into chunks of 500–1000 tokens. This preserves semantic meaning while remaining compatible with modern LLM context windows and downstream analytics workflows.

### Deduplication

SHA-256 content hashing is used to ensure each filing is processed exactly once. This guarantees idempotent execution and prevents redundant computation.

### Auditability and Traceability

Each processed document is tracked with:
- Company CIK  
- Filing type  
- Accession number  
- Content hash  
- Processing timestamp  
- Chunk count  
- Processing status  

This enables full traceability from any derived insight back to its original source.

---

## Testing Strategy

### Unit Tests

- Chunk size and token boundary validation  
- Hash generation and consistency checks  
- PDF and HTML parser correctness  

### Integration Tests

- End-to-end ingestion of sample SEC filings  
- Verification of deduplication logic  
- Validation of document registry state  

---

## Outputs

This case study produces:
- A clean, deduplicated corpus of SEC filings  
- Semantically meaningful, token-bounded text chunks  
- A structured document registry for auditability  
- Evidence-ready data suitable for scoring, analytics, and LLM-based analysis  

---

## Streamlit Application

A Streamlit-based interface can be added to visualize:
- Ingested filings
- Chunked evidence
- Document metadata and registry status

Streamlit App Link:  
<ADD STREAMLIT APP LINK HERE>

---

## Architecture Diagram

The system architecture diagram illustrating ingestion, processing, storage, and data flow can be found below:

Architecture Diagram Link:  
<https://drive.google.com/file/d/1XG-5UuxJHmMyr1j5hF3Ivk0hW5W-XyTm/view>

![Architecture Diagram](architecture.png)


---

## Video Recording

A walkthrough video explaining the system design, pipeline execution, and outputs is available at:

Video Recording Link:  
<ADD VIDEO RECORDING LINK HERE>

---

## How to Run

poetry install

poetry run python src/ingestion/sec_downloader.py  
poetry run python src/parsing/pdf_parser.py  
poetry run python src/parsing/html_parser.py  
poetry run python src/chunking/semantic_chunker.py  

---

## Key Takeaways

- Evidence-backed assessment is mandatory for enterprise AI systems  
- Data engineering dominates real-world AI readiness workflows  
- Reproducibility and auditability are first-class requirements  
- This pipeline mirrors production private equity analytics systems  