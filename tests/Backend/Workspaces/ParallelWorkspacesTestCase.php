<?php
namespace Keboola\Test\Backend\Workspaces;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

abstract class ParallelWorkspacesTestCase extends StorageApiTestCase
{
    /** @var string */
    protected $runId;

    /** @var Client */
    protected $workspaceSapiClient;

    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();

        $description = get_class($this) . '\\' . $this->getName();

        $this->deleteOldTestTokensAndWorkspaces($description);

        $tokenOptions = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription($description)
        ;

        $tokenId = $this->_client->createToken($tokenOptions);
        $tokenData = $this->_client->getToken($tokenId);

        $this->workspaceSapiClient = new Client([
            'token' => $tokenData['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    private function deleteOldTestTokensAndWorkspaces($testName)
    {
        $oldTestTokens = array_filter(
            $this->_client->listTokens(),
            function (array $token) use ($testName) {
                return $token['description'] === $testName;
            }
        );

        foreach ($oldTestTokens as $oldTestToken) {
            $this->_client->dropToken($oldTestToken['id']);
        }

        $workspaces = new Workspaces($this->_client);

        $oldTestWorkspaces = array_filter(
            $workspaces->listWorkspaces(),
            function (array $workspace) use ($testName) {
                return $workspace['creatorToken']['description'] === $testName;
            }
        );

        foreach ($oldTestWorkspaces as $workspace) {
            $workspaces->deleteWorkspace($workspace['id']);
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
