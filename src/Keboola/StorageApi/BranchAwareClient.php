<?php

namespace Keboola\StorageApi;

use InvalidArgumentException;

class BranchAwareClient extends Client
{
    private const START_ENDPOINTS_WITHOUT_BRANCH = [
        'jobs', // get list - jobs are are only in main branch
        'snapshot', // get delete - snapshots are created for all tables in main branch
        'triggers', // post, put, get, list, delete - triggers are not supported in branches
        'tickets', // id generator is only in main branch
    ];
    private const END_ENDPOINTS_WITHOUT_BRANCH = [
        '/alias-filter', // filtered aliases are deprecated and not supported in branches
        '/table-aliases', // table aliases are not supported in branches
    ];

    private string|int $branchId;

    /**
     * Cache config to be able create new instance of Client for default branch
     */
    private array $config;

    /**
     * @param array $config
     */
    public function __construct(int|string $branchId, array $config = [])
    {
        parent::__construct($config);
        if (empty($branchId)) {
            throw new InvalidArgumentException(sprintf('Branch "%s" is not valid.', $branchId));
        }
        $this->branchId = $branchId;
        $this->config = $config;
    }

    private function isUrlSupportedInBranch(string $url): bool
    {
        foreach (self::START_ENDPOINTS_WITHOUT_BRANCH as $endpoint) {
            if (str_starts_with($url, $endpoint)) {
                return false;
            }
        }
        foreach (self::END_ENDPOINTS_WITHOUT_BRANCH as $endpoint) {
            if (str_ends_with($url, $endpoint)) {
                return false;
            }
        }
        return true;
    }

    public function request($method, $url, $options = [], $responseFileName = null, $handleAsyncTask = true)
    {
        if ($this->isUrlSupportedInBranch($url)) {
            $url = 'branch/' . $this->branchId . '/' . $url;
        }

        return parent::request($method, $url, $options, $responseFileName, $handleAsyncTask);
    }

    public function getCurrentBranchId(): int|string
    {
        return $this->branchId;
    }

    public function getDefaultBranchClient(): Client
    {
        return new Client($this->config);
    }
}
