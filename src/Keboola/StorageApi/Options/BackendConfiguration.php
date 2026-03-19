<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Options;

use JsonException;

class BackendConfiguration
{
    public function __construct(
        public readonly ?string $context = null,
        public readonly ?string $size = null,
    ) {
    }

    /**
     * @throws JsonException
     */
    public function toJson(): string
    {
        $payload = [];
        if ($this->context !== null) {
            $payload['context'] = $this->context;
        }
        if ($this->size !== null) {
            $payload['size'] = $this->size;
        }
        return json_encode($payload, JSON_THROW_ON_ERROR);
    }
}
