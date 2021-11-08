<?php

namespace Keboola\Test\ClientProvider;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;

class ClientProvider
{

    const DEFAULT_BRANCH = 'defaultBranch';
    const DEV_BRANCH = 'devBranch';

    /**
     * @var StorageApiTestCase
     */
    private $testCase;

    /**
     * @var scalar
     */
    private $dataProviderKey;

    public function __construct(StorageApiTestCase $testCase, $dataProviderKey = 0)
    {
        $this->testCase = $testCase;
        $this->dataProviderKey = $dataProviderKey;
    }

    // CREATORS

    /**
     * @param array $config
     * @param bool $useExistingBranch
     * @return Client
     */
    public function createClientForCurrentTest($config = [], $useExistingBranch = false)
    {
        if ($this->testCase->usesDataProvider()) {
            if ($this->testCase->getProvidedData()[$this->dataProviderKey] === self::DEFAULT_BRANCH) {
                return $this->getDefaultClient($config);
            } elseif ($this->testCase->getProvidedData()[$this->dataProviderKey] === self::DEV_BRANCH) {
                return $this->getDevBranchClient($config, $useExistingBranch);
            }
        }

        return $this->getDefaultClient();
    }

    /**
     * @param array $config
     * @param bool $useExistingBranch
     * @return BranchAwareClient
     */
    public function createBranchAwareClientForCurrentTest($config = [], $useExistingBranch = false)
    {
        if ($this->testCase->usesDataProvider()) {
            if ($this->testCase->getProvidedData()[$this->dataProviderKey] === self::DEFAULT_BRANCH) {
                return $this->getDefaultBranchClient($config);
            } elseif ($this->testCase->getProvidedData()[$this->dataProviderKey] === self::DEV_BRANCH) {
                return $this->getDevBranchClient($config, $useExistingBranch);
            }
        }

        return $this->getDefaultBranchClient();
    }

    // CLIENTS

    /**
     * @param array $config
     * @return Client
     */
    public function getDefaultClient($config = [])
    {
        if ($config) {
            return $this->testCase->getClient($config);
        } else {
            return $this->testCase->getDefaultClient();
        }
    }

    /**
     * @param array $config
     * @return BranchAwareClient
     */
    public function getDefaultBranchClient($config = [])
    {
        $branchId = $this->testCase->getDefaultBranchId($this->testCase);
        
        if ($config) {
            return $this->testCase->getBranchAwareClient($branchId, $config);
        } else {
            return $this->testCase->getBranchAwareDefaultClient($branchId);
        }
    }

    /**
     * @param array $config
     * @param bool $useExistingBranch
     * @return BranchAwareClient
     */
    public function getDevBranchClient($config = [], $useExistingBranch = false)
    {
        if ($useExistingBranch) {
            $branch = $this->getExistingBranchForTestCase();
        } else {
            $branch = $this->createDevBranchForTestCase();
        }

        if ($config) {
            return $this->testCase->getBranchAwareClient($branch['id'], $config);
        } else {
            return $this->testCase->getBranchAwareDefaultClient($branch['id']);
        }
    }

    // HELPERS

    /**
     * @return string
     */
    public function getDevBranchName()
    {
        $providedToken = $this->getDefaultClient()->verifyToken();
        return implode('\\', [
            __CLASS__,
            $this->testCase->getName(false),
            $this->testCase->dataName(),
            $providedToken['id'],
        ]);
    }

    /**
     * @return array
     */
    public function getExistingBranchForTestCase()
    {
        $branchName = $this->getDevBranchName();
        $devBranch = new DevBranches($this->getDefaultClient());

        $branches = $devBranch->listBranches();
        $branch = null;
        // get branch detail
        foreach ($branches as $branchItem) {
            if ($branchItem['name'] === $branchName) {
                $branch = $branchItem;
            }
        }
        if (!isset($branch)) {
            $this->testCase->fail(sprintf('Reuse existing branch: branch %s not found.', $branchName));
        }

        return $branch;
    }

    /**
     * @return array
     */
    public function createDevBranchForTestCase()
    {
        $branchName = $this->getDevBranchName();
        $devBranch = new DevBranches($this->getDefaultClient());

        $this->testCase->deleteBranchesByPrefix($devBranch, $branchName);
        return $devBranch->createBranch($branchName);
    }
}
