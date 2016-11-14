<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 06/02/14
 * Time: 10:43
 * To change this template use File | Settings | File Templates.
 */

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
    public function setRunId($runId)
    {
        $this->runId = $runId;
        return $this;
    }

    public function toArray()
    {
        return [
            'runId' => $this->getRunId(),
        ];
    }
}
