<?php

namespace Keboola\StorageApi;

class BranchAwareClient extends Client
{
    private $branchId;

    public function __construct($branchId, array $config = [])
    {
        parent::__construct($config);
        if (empty($branchId)) {
            throw new \InvalidArgumentException(sprintf('Branch "%s" is not valid.', $branchId));
        }
        $this->branchId = $branchId;
    }

    public function request($method, $url, $options = array(), $responseFileName = null, $handleAsyncTask = true)
    {
        if (strpos($url, 'jobs/') !== 0) {
            $url = 'branch/' . $this->branchId . '/' . $url;
        }

        return parent::request($method, $url, $options, $responseFileName, $handleAsyncTask);
    }
}
