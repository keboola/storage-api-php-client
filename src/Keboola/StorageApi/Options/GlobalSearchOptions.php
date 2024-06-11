<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Options;

class GlobalSearchOptions
{
    private ?int $limit;

    private ?int $offset;

    /** @var string[]|null */
    private ?array $types;

    /** @var int[]|null  */
    private ?array $projectIds;

    /**
     * @param int[]|null $projectIds
     * @param string[]|null $types
     */
    public function __construct(?int $limit = null, ?int $offset = null, ?array $types = null, ?array $projectIds = null)
    {
        $this->limit = $limit;
        $this->offset = $offset;
        $this->types = $types;
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

        if ($this->types !== null) {
            $params['types'] = $this->types;
        }

        if ($this->projectIds !== null) {
            $params['projectIds'] = $this->projectIds;
        }

        return $params;
    }
}
