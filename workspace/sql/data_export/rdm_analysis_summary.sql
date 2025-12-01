-- ============================================================================
-- Script: rdm_analysis_summary.sql
-- Purpose: Query RDM database to get list of analyses and groups exported
-- Parameters:
--   {{ RDM_NAME }} - Name of the RDM database (e.g., 'RM_RDM_202511_QEM_USAP_DvrH')
-- Returns: List of analysis/group names with their type (analysis or group)
-- Author: IRP Framework
-- ============================================================================

USE [{{ RDM_NAME }}]

SELECT
    NAME,
    ISGROUP
FROM rdm_analysis
ORDER BY ISGROUP, NAME
