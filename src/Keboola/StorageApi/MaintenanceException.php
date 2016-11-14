<?php
namespace Keboola\StorageApi;

class MaintenanceException extends ClientException
{

    /**
     * @var int
     */
    private $retryAfter;

    public function __construct($reason, $retryAfter, $params)
    {
        $this->retryAfter = (int)$retryAfter;
        parent::__construct($reason, 503, null, "MAINTENANCE", $params);
    }

    /**
     * @return int
     */
    public function getRetryAfter()
    {
        return $this->retryAfter;
    }
}
