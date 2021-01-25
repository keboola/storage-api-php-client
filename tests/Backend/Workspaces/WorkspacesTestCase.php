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

abstract class WorkspacesTestCase extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->deleteAllWorkspaces();
    }

    private function deleteAllWorkspaces()
    {
        $workspaces = new Workspaces($this->_client);
        foreach ($workspaces->listWorkspaces() as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [
                'async' => true,
            ]);
        }
    }

    protected function getDbConnection($connection)
    {
        switch ($connection['backend']) {
            case parent::BACKEND_SNOWFLAKE:
                $db = new Connection([
                    'host' => $connection['host'],
                    'database' => $connection['database'],
                    'warehouse' => $connection['warehouse'],
                    'user' => $connection['user'],
                    'password' => $connection['password'],
                ]);
                // set connection to use workspace schema
                $db->query(sprintf("USE SCHEMA %s;", $db->quoteIdentifier($connection['schema'])));

                return $db;
            case parent::BACKEND_REDSHIFT:
                $pdo = new \PDO(
                    "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
                    $connection['user'],
                    $connection['password']
                );
                $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

                return $pdo;
            case parent::BACKEND_SYNAPSE:
                $db = \Doctrine\DBAL\DriverManager::getConnection([
                    'user' => $connection['user'],
                    'password' => $connection['password'],
                    'host' => $connection['host'],
                    'dbname' => $connection['database'],
                    'port' => 1433,
                    'driver' => 'pdo_sqlsrv',
                    'driverOptions' => [
                        'LoginTimeout' => 30,
                        'ConnectRetryCount' => 5,
                        'ConnectRetryInterval' => 10,
                    ],
                ]);
                $db->connect();

                return $db;
        }

        throw new \Exception("Unsupported Backend for workspaces");
    }
}
