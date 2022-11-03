<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Options;

class BackendConfiguration
{
    private ?string $context;

    private ?string $size;

    public function __construct(
        ?string $context = null,
        ?string $size = null
    ) {
        $this->context = $context;
        $this->size = $size;
    }

    public function withSize(?string $size): self
    {
        $this->size = $size;
        return $this;
    }

    public function withContext(?string $context): self
    {
        $this->context = $context;
        return $this;
    }

    /**
     * @throws \JsonException
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
