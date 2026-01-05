<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Options;

use InvalidArgumentException;

class BucketOwnerUpdateOptions
{
    private ?int $id;
    private ?string $email;

    public function __construct(?int $id = null, ?string $email = null)
    {
        $this->id = $id;
        $this->email = $email;

        if ($this->id !== null && $this->email !== null) {
            throw new InvalidArgumentException('Only one attribute is accepted. Fill "id" or "email" not both.');
        }

        if ($this->id === null && $this->email === null) {
            throw new InvalidArgumentException('At least one attribute must be specified. Fill "id" or "email".');
        }
    }

    public function toArray(): array
    {
        $payload = [];
        if ($this->id !== null) {
            $payload['id'] = $this->id;
        }
        if ($this->email !== null) {
            $payload['email'] = $this->email;
        }
        return $payload;
    }
}
