<?php
/**
 * Created by PhpStorm.
 * User: marc
 * Date: 04/07/2016
 * Time: 10:52
 */

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

class WorkspaceTestCase extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->deleteAllWorkspaces();
    }

    private function deleteAllWorkspaces()
    {
        $workspaces  = new Workspaces($this->_client);
        foreach ($workspaces->listWorkspaces() as $workspace) {
            $workspaces->deleteWorkspace($workspace['id']);
        }
    }

    protected function getDbConnection($connection)
    {
        if ($connection['backend'] === parent::BACKEND_SNOWFLAKE) {

            return new Connection([
                'host' => $connection['host'],
                'database' => $connection['database'],
                'warehouse' => $connection['warehouse'],
                'user' => $connection['user'],
                'password' => $connection['password'],
            ]);

        } else if ($connection['backend'] === parent::BACKEND_REDSHIFT) {

            return new \PDO(
                "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
                $connection['user'],
                $connection['password']
            );

        } else {
            throw new Exception("Unsupported Backend for workspaces");
        }
    }
}