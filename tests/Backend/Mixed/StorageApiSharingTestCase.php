<?php

namespace Keboola\Test\Backend\Mixed;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

abstract class StorageApiSharingTestCase extends StorageApiTestCase
{
    const TEST_METADATA_PROVIDER = 'test-metadata-provider';

    /**
     * @var Client
     */
    protected $_client2;

    protected $clientAdmin2InSameOrg;
    protected $clientAdmin3InOtherOrg;

    public function setUp()
    {
        parent::setUp();


        $this->_client2 = new Client([
            'token' => STORAGE_API_LINKING_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ]);

        $this->clientAdmin2InSameOrg = new Client([
            'token' => STORAGE_API_TOKEN_ADMIN_2_IN_SAME_ORGANIZATION,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ]);

        $this->clientAdmin3InOtherOrg = new Client([
            'token' => STORAGE_API_TOKEN_ADMIN_3_IN_OTHER_ORGANIZATION,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ]);

        $tokenData = $this->_client->verifyToken();
        $tokenAdmin2InSameOrgData = $this->clientAdmin2InSameOrg->verifyToken();
        $tokenAdmin3InOtherOrg = $this->clientAdmin3InOtherOrg->verifyToken();

        // not same admins validation
        $adminIds = [
            'STORAGE_API_TOKEN' => $tokenData['admin']['id'],
            'STORAGE_API_TOKEN_ADMIN_2_IN_SAME_ORGANIZATION' => $tokenAdmin2InSameOrgData['admin']['id'],
            'STORAGE_API_TOKEN_ADMIN_3_IN_OTHER_ORGANIZATION' => $tokenAdmin3InOtherOrg['admin']['id'],
        ];

        if (count(array_unique($adminIds)) !== count($adminIds)) {
            throw new \Exception(sprintf(
                'Tokens %s cannot belong to the same admin', implode(', ', array_keys($adminIds))
            ));
        }

        // same organizations validation
        if ($tokenData['organization']['id'] !== $this->_client2->verifyToken()['organization']['id']) {
            throw new \Exception("STORAGE_API_LINKING_TOKEN is not in the same organization as STORAGE_API_TOKEN");
        } elseif ($tokenData['organization']['id'] !== $tokenAdmin2InSameOrgData['organization']['id']) {
            throw new \Exception(
                "STORAGE_API_TOKEN_ADMIN_2_IN_SAME_ORGANIZATION is not in the same organization as STORAGE_API_TOKEN"
            );
        }

        // not same organization
        if ($tokenData['organization']['id'] === $tokenAdmin3InOtherOrg['organization']['id']) {
            throw new \Exception(
                "STORAGE_API_TOKEN_ADMIN_3_IN_OTHER_ORGANIZATION is in the same organization as STORAGE_API_TOKEN"
            );
        }
    }

    /**
     * Remove all workspaces in both projects
     */
    protected function deleteAllWorkspaces()
    {
        /**
         * @var Client[] $clients
         */
        $clients = [
            $this->_client,
            $this->_client2,
        ];

        // unlink buckets
        foreach ($clients as $client) {
            $workspaces = new Workspaces($client);
            foreach ($workspaces->listWorkspaces() as $workspace) {
                $workspaces->deleteWorkspace($workspace['id']);
            }
        }
    }

    /**
     * Init empty bucket test helper
     *
     * @param $name
     * @param $stage
     * @return bool|string
     */
    private function initEmptyBucket($name, $stage, $backend)
    {
        if ($this->_client->bucketExists("$stage.c-$name")) {
            $this->_client->dropBucket(
                "$stage.c-$name",
                [
                    'force' => true,
                ]
            );
        }

        return $this->_client->createBucket($name, $stage, 'Api tests', $backend);
    }

    /**
     * Unlinks and unshare all buckets from both projects
     * Then recreates test buckets in given backend
     *
     * @param $backend
     * @return array created bucket ids
     * @throws ClientException
     */
    protected function initTestBuckets($backend)
    {
        /**
         * @var Client[] $clients
         */
        $clients = [
            $this->_client,
            $this->_client2,
        ];

        // unlink buckets
        foreach ($clients as $client) {
            foreach ($client->listBuckets() as $bucket) {
                if (!empty($bucket['sourceBucket'])) {
                    $client->dropBucket($bucket['id']);
                }
            }
        }

        // unshare buckets
        foreach ($clients as $client) {
            foreach ($client->listBuckets() as $bucket) {
                if ($client->isSharedBucket($bucket['id'])) {
                    $client->unshareBucket($bucket['id']);
                }
            }
        }

        // recreate buckets in firs project
        $this->_bucketIds = [];
        foreach ([self::STAGE_OUT, self::STAGE_IN] as $stage) {
            $this->_bucketIds[$stage] = $this->initEmptyBucket('API-sharing', $stage, $backend);
        }

        return $this->_bucketIds;
    }

    /**
     * @param $connection
     * @return Connection|\PDO
     * @throws \Exception
     */
    protected function getDbConnection($connection)
    {
        if ($connection['backend'] === parent::BACKEND_SNOWFLAKE) {
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
        } else if ($connection['backend'] === parent::BACKEND_REDSHIFT) {
            $pdo = new \PDO(
                "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
                $connection['user'],
                $connection['password']
            );
            $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            return $pdo;
        } else {
            throw new \Exception("Unsupported Backend for workspaces");
        }
    }

    public function sharingBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT],
        ];
    }

    public function workspaceMixedBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT],
        ];
    }
}
