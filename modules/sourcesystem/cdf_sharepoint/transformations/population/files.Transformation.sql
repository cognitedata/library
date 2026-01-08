select
    concat('VAL_', name) as externalId,
    name,
    description as description,
    name AS sourceId,
    'Files' as sourceContext,
    mime_type as mimeType,
    array('DetectInDiagrams', 'ToAnnotate') as tags,
    array(regexp_replace(name, '\\.[^.]+$', '')) as aliases
from `{{ rawSourceDatabase }}`.`files_metadata`
where
    isnotnull(mime_type)
    and mime_type = 'application/pdf'
