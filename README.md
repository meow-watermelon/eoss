# Eric's Object Storage Service

## Intro

Eric's Object Storage Service(EOSS) is my experimental object storage service. I designed EOSS with following features:

* support object versioning
* support HTTP HEAD/GET/PUT/DELETE methods for object operations
* object metadata and object files should be easy to move around

EOSS uses local filesystem as the storage layer.

## Architecture

EOSS has 3 pieces of components: EOSS Service, Metadata Database and Storage Layer. EOSS Service is written by Python 3. Metadata Database uses SQLite to store the object metadata. Storage Layer is local filesystem or local-mounted NFS share.
