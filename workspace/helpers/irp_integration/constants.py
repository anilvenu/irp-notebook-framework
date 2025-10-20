# API Endpoint Constants

# Workflow endpoints
GET_WORKFLOWS = '/riskmodeler/v1/workflows'

# Storage endpoints
CREATE_AWS_BUCKET = '/riskmodeler/v1/storage'

# Import endpoints
CREATE_MAPPING = '/riskmodeler/v1/imports/createmapping/{bucket_id}'
EXECUTE_IMPORT = '/riskmodeler/v1/imports'

# Currency endpoints
GET_CURRENCIES = '/riskmodeler/v1/domains/Client/tablespace/UserConfig/entities/currency/values'

# Tag endpoints
GET_TAGS = '/data-store/referencedata/v1/tags'
CREATE_TAG = '/data-store/referencedata/v1/tags'

# Export endpoints
EXPORT_JOB = '/platform/export/v1/jobs'

# Portfolio endpoints
CREATE_PORTFOLIO = '/riskmodeler/v2/portfolios'
GET_PORTFOLIOS = '/riskmodeler/v2/portfolios'
GET_PORTFOLIO_BY_ID = '/riskmodeler/v2/portfolios/{portfolio_id}'
PORTFOLIO_GEOHAZ = '/riskmodeler/v2/portfolios/{portfolio_id}/geohaz'
PORTFOLIO_PROCESS = '/riskmodeler/v2/portfolios/{portfolio_id}/process'

# EDM/Datasource endpoints
GET_DATASOURCES = '/riskmodeler/v2/datasources'
CREATE_DATASOURCE = '/riskmodeler/v2/datasources'
DELETE_DATASOURCE = '/riskmodeler/v2/datasources/{edm_name}'
EXPORT_EDM = '/riskmodeler/v2/exports'
GET_CEDANTS = '/riskmodeler/v1/cedants'
GET_LOBS = '/riskmodeler/v1/lobs'

# Analysis endpoints
GET_MODEL_PROFILES = '/analysis-settings/modelprofiles'
GET_ANALYSES = '/riskmodeler/v2/analyses'
GET_OUTPUT_PROFILES = '/analysis-settings/outputprofiles'
GET_EVENT_RATE_SCHEME = '/data-store/referencetables/eventratescheme'
GET_PLATFORM_ANALYSES = '/platform/riskdata/v1/analyses'
CREATE_ANALYSIS_GROUP = '/riskmodeler/v2/analysis-groups'

# Treaty endpoints
GET_TREATIES = '/riskmodeler/v1/treaties'
CREATE_TREATY = '/riskmodeler/v1/treaties'
TREATY_LOB_BATCH = '/riskmodeler/v1/treaties/lob/batch'
GET_TREATY_TYPES = '/riskmodeler/v1/domains/RMS/tablespace/System/entities/TreatyType/values'
GET_TREATY_ATTACHMENT_BASES = '/riskmodeler/v1/domains/RMS/tablespace/System/entities/AttachBasis/values'
GET_TREATY_ATTACHMENT_LEVELS = '/riskmodeler/v1/domains/RMS/tablespace/System/entities/AttachLevel/values'
