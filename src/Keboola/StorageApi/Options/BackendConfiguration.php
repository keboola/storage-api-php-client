<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Options;

class BackendConfiguration
{
    private string $context;

    private string $size;

    public function __construct(
        string $context,
        string $size
    ) {
        $this->context = $context;
        $this->size = $size;
    }

    /**
     * @throws \JsonException
     */
    public function toJson(): string
    {
        return json_encode([
            'context' => $this->context,
            'size' => $this->size,
        ], JSON_THROW_ON_ERROR);
    }
}
