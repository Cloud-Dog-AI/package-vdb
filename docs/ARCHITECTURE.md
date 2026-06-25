# cloud_dog_vdb Architecture

## Purpose
`cloud_dog_vdb` provides shared vector database, embedding, ingestion, and retrieval abstractions for Cloud-Dog Python services.

## Main responsibilities
- wrap multiple vector stores behind a common client interface
- support document ingestion, embedding, chunking, and retrieval
- provide lifecycle helpers for remote and embedded providers
- integrate with shared LLM, jobs, and configuration packages

## Main components
- provider clients and factories
- ingestion and chunking helpers
- remote lifecycle and health helpers
- search and record management models
