<?php

namespace Keboola\StorageApi;

class BranchAwareClient extends Client
{
    /** @var int|string */
    private $branchId;

    /**
     * @param int|string $branchId
     * @param array $config
     */
    public function __construct($branchId, array $config = [])
    {
        parent::__construct($config);
        if (empty($branchId)) {
            throw new \InvalidArgumentException(sprintf('Branch "%s" is not valid.', $branchId));
        }
        $this->branchId = $branchId;
    }

    public function request($method, $url, $options = [], $responseFileName = null, $handleAsyncTask = true)
    {
        if (strpos($url, 'jobs/') !== 0) {
            $url = 'branch/' . $this->branchId . '/' . $url;
        }

        return parent::request($method, $url, $options, $responseFileName, $handleAsyncTask);
    }

    /**
     * @return int|string
     */
    public function getCurrentBranchId()
    {
        return $this->branchId;
    }
}
