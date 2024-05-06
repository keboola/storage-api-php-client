<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Options;

class GlobalSearchOptions
{
    private ?int $limit;

    private ?int $offset;

    private ?string $type;

    /** @var int[]|null  */
    private ?array $projectIds;

    /**
     * @param int[]|null $projectIds
     */
    public function __construct(?int $limit = null, ?int $offset = null, ?string $type = null, ?array $projectIds = null)
    {
        $this->limit = $limit;
        $this->offset = $offset;
        $this->type = $type;
        $this->projectIds = $projectIds;
    }

    public function toParamsArray(): array
    {
        $params = [];

        if ($this->limit !== null) {
            $params['limit'] = $this->limit;
        }

        if ($this->offset !== null) {
            $params['offset'] = $this->offset;
        }

        if ($this->type !== null) {
            $params['type'] = $this->type;
        }

        if ($this->projectIds !== null) {
            $params['projectIds'] = $this->projectIds;
        }

        return $params;
    }
}
