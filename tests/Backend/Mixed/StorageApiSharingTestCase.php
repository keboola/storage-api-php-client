<?php

namespace Keboola\Test\Backend\Mixed;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Tokens;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

abstract class StorageApiSharingTestCase extends StorageApiTestCase
{
    const TEST_METADATA_PROVIDER = 'test-metadata-provider';

    /** @var Client */
    protected $_client2;

    /** @var Tokens */
    protected $tokensInLinkingProject;

    /** @var Client */
    protected $clientAdmin2InSameOrg;

    /** @var Client */
    protected $clientAdmin3InOtherOrg;

    /** @var Client */
    protected $shareRoleClient;

    public function setUp()
    {
        parent::setUp();

        $this->_client2 = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN
        );

        $this->tokensInLinkingProject = new Tokens($this->_client2);

        $this->clientAdmin2InSameOrg = $this->getClientForToken(
            STORAGE_API_TOKEN_ADMIN_2_IN_SAME_ORGANIZATION
        );

        $this->clientAdmin3InOtherOrg = $this->getClientForToken(
            STORAGE_API_TOKEN_ADMIN_3_IN_OTHER_ORGANIZATION
        );

        $this->shareRoleClient = $this->getClientForToken(
            STORAGE_API_SHARE_TOKEN
        );

        $tokenData = $this->_client->verifyToken();
        $tokenAdmin2InSameOrgData = $this->clientAdmin2InSameOrg->verifyToken();
        $tokenAdmin3InOtherOrg = $this->clientAdmin3InOtherOrg->verifyToken();
        $shareRoleTokenData = $this->shareRoleClient->verifyToken();

        // not same admins validation
        $adminIds = [
            'STORAGE_API_TOKEN' => $tokenData['admin']['id'],
            'STORAGE_API_TOKEN_ADMIN_2_IN_SAME_ORGANIZATION' => $tokenAdmin2InSameOrgData['admin']['id'],
            'STORAGE_API_TOKEN_ADMIN_3_IN_OTHER_ORGANIZATION' => $tokenAdmin3InOtherOrg['admin']['id'],
            'STORAGE_API_SHARE_TOKEN' => $shareRoleTokenData['admin']['id'],
        ];

        if (count(array_unique($adminIds)) !== count($adminIds)) {
            throw new \Exception(sprintf(
                'Tokens %s cannot belong to the same admin',
                implode(
                    ', ',
                    array_keys($adminIds)
                )
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
    protected function initEmptyBucketForSharingTest($name, $stage, $backend)
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
            $this->clientAdmin2InSameOrg,
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
            $this->_bucketIds[$stage] = $this->initEmptyBucketForSharingTest('API-sharing', $stage, $backend);
        }

        return $this->_bucketIds;
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
