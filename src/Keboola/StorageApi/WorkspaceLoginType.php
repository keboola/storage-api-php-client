<?php

declare(strict_types=1);

namespace Keboola\StorageApi;

enum WorkspaceLoginType: string
{
    case DEFAULT = 'default';
    case SNOWFLAKE_PERSON_SSO = 'snowflake-person-sso';
    case SNOWFLAKE_LEGACY_SERVICE_PASSWORD = 'snowflake-legacy-service';
}
