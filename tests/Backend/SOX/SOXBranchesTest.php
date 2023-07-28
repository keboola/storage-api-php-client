<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;

class SOXBranchesTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->cleanupTestBranches($this->getDeveloperStorageApiClient());
    }
}
