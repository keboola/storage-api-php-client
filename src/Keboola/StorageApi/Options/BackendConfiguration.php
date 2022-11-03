<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Options;

class BackendConfiguration
{
    private string $context;

    private string $size;

    public function __construct(
        ?string $context = null,
        ?string $size = null
    ) {
        $this->context = $context;
        $this->size = $size;
    }

    public function withSize(string $size): self
    {
        $this->size = $size;
        return $this;
    }

    public function withContext(string $context): self
    {
        $this->context = $context;
        return $this;
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
