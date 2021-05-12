<?php

namespace Keboola\StorageApi;

class BranchAwareClient extends Client
{
    private $branch;

    public function __construct($branch, array $config = [])
    {
        parent::__construct($config);
        if (empty($branch)) {
            throw new \InvalidArgumentException(sprintf('Branch "%s" is not valid.', $branch));
        }
        $this->branch = $branch;
    }

    public function request($method, $url, $options = array(), $responseFileName = null, $handleAsyncTask = true)
    {
        if (strpos($url, 'jobs/') !== 0) {
            $url = 'branch/' . $this->branch . '/' . $url;
        }

        return parent::request($method, $url, $options, $responseFileName, $handleAsyncTask);
    }
}
