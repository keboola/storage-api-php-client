<?php

namespace Keboola\StorageApi;

class BranchAwareClient extends Client
{
    private $branch;

    public function __construct($branch, array $config = [])
    {
        parent::__construct($config);
        $this->branch = $branch;
    }

    public function request($method, $url, $options = array(), $responseFileName = null, $handleAsyncTask = true)
    {
        $url = 'branch/' . $this->branch . '/' . $url;
        return parent::request($method, $url, $options, $responseFileName, $handleAsyncTask);
    }
}
