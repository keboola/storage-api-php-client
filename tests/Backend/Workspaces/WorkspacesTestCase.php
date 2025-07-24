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
    public function setUp(bool $deleteWorkspaces = true): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        if ($deleteWorkspaces) {
            $this->deleteAllWorkspaces();
        }
    }

    private function deleteAllWorkspaces()
    {
        $workspaces = new Workspaces($this->_client);
        foreach ($workspaces->listWorkspaces() as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [], true);
        }
    }
}
