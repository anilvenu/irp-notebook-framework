# API Endpoint Constants

# Workflow endpoints
GET_WORKFLOWS = '/riskmodeler/v1/workflows'

# EDM/Datasource endpoints
GET_DATASOURCES = '/riskmodeler/v2/datasources'
CREATE_DATASOURCE = '/riskmodeler/v2/datasources'
DELETE_DATASOURCE = '/riskmodeler/v2/datasources/{edm_name}'
EXPORT_EDM = '/riskmodeler/v2/exports'
GET_CEDANTS = '/riskmodeler/v1/cedants'
GET_LOBS = '/riskmodeler/v1/lobs'

# MRI Import Endpoints
CREATE_AWS_BUCKET = '/riskmodeler/v1/storage'
CREATE_MAPPING = '/riskmodeler/v1/imports/createmapping/{bucket_id}'
EXECUTE_IMPORT = '/riskmodeler/v1/imports'

# Portfolio endpoints
CREATE_PORTFOLIO = '/riskmodeler/v2/portfolios'
GET_PORTFOLIOS = '/riskmodeler/v2/portfolios'
GET_PORTFOLIO_BY_ID = '/riskmodeler/v2/portfolios/{portfolio_id}'
PORTFOLIO_GEOHAZ = '/riskmodeler/v2/portfolios/{portfolio_id}/geohaz'
ANALYZE_PORTFOLIO = '/riskmodeler/v2/portfolios/{portfolio_id}/process'

# Treaty endpoints
GET_TREATIES = '/riskmodeler/v1/treaties'
CREATE_TREATY = '/riskmodeler/v1/treaties'
ASSIGN_TREATY_LOBS = '/riskmodeler/v1/treaties/lob/batch'
GET_TREATY_TYPES = '/riskmodeler/v1/domains/RMS/tablespace/System/entities/TreatyType/values'
GET_TREATY_ATTACHMENT_BASES = '/riskmodeler/v1/domains/RMS/tablespace/System/entities/AttachBasis/values'
GET_TREATY_ATTACHMENT_LEVELS = '/riskmodeler/v1/domains/RMS/tablespace/System/entities/AttachLevel/values'

# Analysis endpoints
GET_MODEL_PROFILES = '/analysis-settings/modelprofiles'
GET_OUTPUT_PROFILES = '/analysis-settings/outputprofiles'
GET_EVENT_RATE_SCHEME = '/data-store/referencetables/eventratescheme'
GET_PLATFORM_ANALYSES = '/platform/riskdata/v1/analyses'
GET_ANALYSES = '/riskmodeler/v2/analyses'
CREATE_ANALYSIS_GROUP = '/riskmodeler/v2/analysis-groups'

# Tag endpoints
GET_TAGS = '/data-store/referencedata/v1/tags'
CREATE_TAG = '/data-store/referencedata/v1/tags'

# RDM endpoints
EXPORT_TO_RDM = '/platform/export/v1/jobs'

# Currency endpoints
GET_CURRENCIES = '/riskmodeler/v1/domains/Client/tablespace/UserConfig/entities/currency/values'
