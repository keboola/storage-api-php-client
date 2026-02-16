<?php

namespace Keboola\StorageApi\Options;

class StatsOptions
{
    private $runId;

    /**
     * @return mixed
     */
    public function getRunId()
    {
        return $this->runId;
    }

    /**
     * @param $runId
     * @return $this
     */
    public function setRunId($runId): static
    {
        $this->runId = $runId;
        return $this;
    }

    public function toArray(): array
    {
        return [
            'runId' => $this->getRunId(),
        ];
    }
}
