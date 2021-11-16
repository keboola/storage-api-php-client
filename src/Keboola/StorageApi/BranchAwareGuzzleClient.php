<?php

namespace Keboola\StorageApi;

use \GuzzleHttp\Client;

class BranchAwareGuzzleClient extends Client
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

    public function request($method, $uri = '', array $options = [])
    {
        if (strpos($uri, '/v2/storage/') === 0 && strpos($uri, 'jobs/') !== 0) {
            $uri = substr_replace($uri, sprintf('branch/%s/', $this->branchId), strlen('/v2/storage/'), 0);
        }

        return parent::request($method, $uri, $options);
    }

    /**
     * @return mixed
     */
    public function getCurrentBranchId()
    {
        return $this->branchId;
    }
}
