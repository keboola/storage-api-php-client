<?php

/**
 * Created by PhpStorm.
 * User: marc
 * Date: 04/07/2016
 * Time: 10:52
 */

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

abstract class WorkspacesTestCase extends StorageApiTestCase
{
    use DeleteWorkspacesTrait;

    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }
}
