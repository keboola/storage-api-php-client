<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use SensitiveParameter;

/**
 * @internal
 * @todo refactor this class to shared lib
 */
class PemKeyCertificatePair
{
    public function __construct(
        #[SensitiveParameter] private readonly string $privateKey,
        private readonly string $publicKey,
    ) {
    }

    public function getPrivateKey(): string
    {
        return $this->privateKey;
    }

    public function getPublicKey(): string
    {
        return $this->publicKey;
    }
}
