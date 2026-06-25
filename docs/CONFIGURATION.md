# cloud_dog_vdb Configuration

## Typical inputs
Consumer services typically configure:
- vector database provider
- provider connection URL or host
- collection or index names
- embedding provider and model
- ingestion limits and chunk sizing
- remote client timeouts and retry values

## Guidance
- keep provider credentials out of source files
- drive provider selection through config, not code constants
- separate publishable unit-test data from internal integration backends
