<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Workspaces;

class ResetCredentialsRequest
{
    public function __construct(
        public readonly ?string $publicKey = null,
    ) {
    }
}
