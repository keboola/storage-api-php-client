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

    /** @var string[]|null  */
    private ?array $branchTypes;

    /** @var int[]|null  */
    private ?array $branchIds;

    /**
     * @param int[]|null $projectIds
     * @param string[]|null $types
     * @param string[]|null $branchTypes
     * @param int[]|null $branchIds
     */
    public function __construct(
        ?int $limit = null,
        ?int $offset = null,
        ?array $types = null,
        ?array $projectIds = null,
        ?array $branchTypes = null,
        ?array $branchIds = null
    ) {
        $this->limit = $limit;
        $this->offset = $offset;
        $this->types = $types;
        $this->projectIds = $projectIds;
        $this->branchTypes = $branchTypes;
        $this->branchIds = $branchIds;
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

        if ($this->branchTypes !== null) {
            $params['branchTypes'] = $this->branchTypes;
        }

        if ($this->branchIds !== null) {
            $params['branchIds'] = $this->branchIds;
        }

        return $params;
    }
}
