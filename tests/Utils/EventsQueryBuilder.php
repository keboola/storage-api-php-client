<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

class EventsQueryBuilder
{
    private array $params = [];

    public function setTokenId(string $tokenId): self
    {
        $this->params[] = 'token.id:'. $tokenId;

        return $this;
    }

    public function setEvent(string $event): self
    {
        $this->params[] = 'event:' . $event;

        return $this;
    }

    public function setObjectId(string $objectId): self
    {
        $this->params[] = 'objectId:' . $objectId;

        return $this;
    }

    public function setIdBranch(string $idBranch): self
    {
        $this->params[] = 'idBranch:' . $idBranch;

        return $this;
    }

    public function setComponent(string $component): self
    {
        $this->params[] = 'component:' . $component;

        return $this;
    }

    public function setRunId(string $runId): self
    {
        $this->params[] = 'runId:' . $runId;

        return $this;
    }

    public function setProjectId(string $projectId): self
    {
        $this->params[] = 'project.id:' . $projectId;

        return $this;
    }

    public function setObjectType(string $objectType): self
    {
        $this->params[] = 'objectType:' . $objectType;

        return $this;
    }

    public function generateQuery(): string
    {
        return implode(' AND ', $this->params);
    }
}
