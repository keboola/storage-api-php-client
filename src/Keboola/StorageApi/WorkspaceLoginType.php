<?php

declare(strict_types=1);

namespace Keboola\StorageApi;

enum WorkspaceLoginType: string
{
    case DEFAULT = 'default';
    case SNOWFLAKE_PERSON_SSO = 'snowflake-person-sso';
    case SNOWFLAKE_LEGACY_SERVICE_PASSWORD = 'snowflake-legacy-service';
    case SNOWFLAKE_PERSON_KEYPAIR = 'snowflake-person-keypair';
    case SNOWFLAKE_SERVICE_KEYPAIR = 'snowflake-service-keypair';

    public function isPasswordLogin(): bool
    {
        return match ($this) {
            self::DEFAULT,
            self::SNOWFLAKE_LEGACY_SERVICE_PASSWORD => true,
            default => false,
        };
    }

    public function isKeyPairLogin(): bool
    {
        return match ($this) {
            self::SNOWFLAKE_PERSON_KEYPAIR,
            self::SNOWFLAKE_SERVICE_KEYPAIR => true,
            default => false,
        };
    }
}
