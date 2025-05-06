<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Workspaces;

final class SetPublicKeyRequest
{
    public function __construct(
        public string $publicKey,
        public ?PublicKeyName $keyName = null,
    ) {
    }
}
