<?php

declare(strict_types=1);

namespace Keboola\StorageApi;

enum WorkspaceLoginType: string
{
    case DEFAULT = 'snowflake-service-keypair';
    case SNOWFLAKE_PERSON_SSO = 'snowflake-person-sso';
    case SNOWFLAKE_LEGACY_SERVICE_PASSWORD = 'snowflake-legacy-service';
    case SNOWFLAKE_PERSON_KEYPAIR = 'snowflake-person-keypair';

    public const SNOWFLAKE_SERVICE_KEYPAIR = self::DEFAULT;

    public function isPasswordLogin(): bool
    {
        return match ($this) {
            self::SNOWFLAKE_LEGACY_SERVICE_PASSWORD => true,
            default => false,
        };
    }

    public function isKeyPairLogin(): bool
    {
        return match ($this) {
            self::DEFAULT,
            self::SNOWFLAKE_PERSON_KEYPAIR => true,
            default => false,
        };
    }
}
