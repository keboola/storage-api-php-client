<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\ClientProvider\TestSetupHelper;
use Keboola\Test\Utils\ConnectionUtils;

class DenyExternalBucketsInInputMappingTest extends BaseExternalBuckets
{
    use ConnectionUtils;
    protected \Keboola\ManageApi\Client $manageClient;

    private const PROJECT_FEATURE_DENY_EXTERNAL_DATASET_INPUT_MAPPING = 'deny-external-dataset-input-mapping';

    public function setUp(): void
    {
        parent::setUp();

        $this->initEmptyTestBucketsForParallelTests();

        $token = $this->_client->verifyToken();

        $this->assertManageTokensPresent();
        $this->manageClient = $this->getDefaultManageClient();

//        if (!in_array('external-buckets', $token['owner']['features'])) {
//        OR is BYODB backend - as we don't have this flag anywhere I don't feel like adding it only for test
//            $this->markTestSkipped(sprintf('External buckets are not enabled for project "%s"', $token['owner']['id']));
//        }

        $this->allowTestForBackendsOnly([self::BACKEND_SNOWFLAKE], 'Backend has to support external buckets');
    }

    public function testLoadExternalBucketIntoWorkspaceWithoutFeatureFails(): void
    {
        $token = $this->_client->verifyToken();
        if (!in_array(self::PROJECT_FEATURE_DENY_EXTERNAL_DATASET_INPUT_MAPPING, $token['owner']['features'], true)) {
            $this->manageClient->addProjectFeature($token['owner']['id'], self::PROJECT_FEATURE_DENY_EXTERNAL_DATASET_INPUT_MAPPING);
        }

        $workspaces = new Workspaces($this->_client);

        // prepare workspace
        $workspace = $workspaces->createWorkspace();
        $externalBucketPath = [$workspace['connection']['database'], $workspace['connection']['schema']];
        $externalTableName = 'test_table';

        // add first table to workspace with long name, table should be skipped
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // prepare test table
        $db->dropTableIfExists($externalTableName);
        $db->createTable(
            $externalTableName,
            [
                'AMOUNT' => 'INT',
                'DESCRIPTION' => 'STRING',
            ],
        );

        $workspace0 = $workspaces->createWorkspace(['backend' => 'snowflake']);

        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_client, $bucketId);

        $this->_client->registerBucket(
            $testBucketName,
            $externalBucketPath,
            self::STAGE_IN,
            $description,
            'snowflake',
            'testLoadExternalBucketIntoWorkspaceWithoutFeatureFails',
            true,
        );

        try {
            $workspaces->loadWorkspaceData($workspace0['id'], [
                'input' => [
                    [
                        'source' => $bucketId . '.'.$externalTableName,
                        'destination' => 'TEST_TABLE_DESTINATION',
                    ],
                ],
            ]);
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame('workspace.loadRequestBadInput', $e->getStringCode(), $e->getMessage());
            $this->assertStringContainsString(
                'do not have enabled external schema support in Input Mapping',
                $e->getMessage(),
            );
        }

        $this->_client->dropBucket($bucketId);
        $workspaces->deleteWorkspace($workspace0['id']);
        $this->manageClient->removeProjectFeature($token['owner']['id'], self::PROJECT_FEATURE_DENY_EXTERNAL_DATASET_INPUT_MAPPING);
    }

    public function testLoadExternalBucketIntoWorkspace(): void
    {
        $token = $this->_client->verifyToken();
        if (!in_array(self::PROJECT_FEATURE_DENY_EXTERNAL_DATASET_INPUT_MAPPING, $token['owner']['features'], true)) {
            $this->manageClient->addProjectFeature($token['owner']['id'], self::PROJECT_FEATURE_DENY_EXTERNAL_DATASET_INPUT_MAPPING);
        }

        $workspaces = new Workspaces($this->_client);
        $workspace0 = $workspaces->createWorkspace(['backend' => 'snowflake']);
        $projectRole = $workspace0['connection']['database'];

        $this->grantImportedPrivilegesToProjectRole($projectRole);

        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_client, $bucketId);

        $this->_client->registerBucket(
            $testBucketName,
            explode('.', $this->getInboundSharedDatabaseName()),
            self::STAGE_IN,
            $description,
            'snowflake',
            $this->getName(),
            true,
        );

        $registeredBucket = $this->_client->getBucket($bucketId);
        $this->assertTrue($registeredBucket['isSnowflakeSharedDatabase']);

        try {
            $workspaces->loadWorkspaceData($workspace0['id'], [
                'input' => [
                    [
                        'source' => $bucketId . '.NAMES_TABLE',
                        'destination' => 'NAMES_TABLE_DESTINATION',
                    ],
                ],
            ]);
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertStringContainsString(
                'do not have enabled external schema support in Input Mapping',
                $e->getMessage(),
            );
        }

        $this->_client->dropBucket($bucketId);
        $workspaces->deleteWorkspace($workspace0['id']);
        $this->manageClient->removeProjectFeature($token['owner']['id'], self::PROJECT_FEATURE_DENY_EXTERNAL_DATASET_INPUT_MAPPING);
    }

    private function grantImportedPrivilegesToProjectRole(string $projectRole): void
    {
        $db = $this->ensureSnowflakeConnection();
        $db->executeQuery('USE ROLE ACCOUNTADMIN');
        $db->executeQuery(sprintf(
            'GRANT IMPORTED PRIVILEGES ON DATABASE %s TO %s',
            explode('.', $this->getInboundSharedDatabaseName())[0],
            $projectRole,
        ));
    }

    private function getInboundSharedDatabaseName(): string
    {
        $inboundDatabaseName = getenv('SNOWFLAKE_INBOUND_DATABASE_NAME');
        assert($inboundDatabaseName !== false, 'SNOWFLAKE_INBOUND_DATABASE_NAME env var is not set');
        $this->assertCount(
            2,
            explode('.', $inboundDatabaseName),
            sprintf('SNOWFLAKE_INBOUND_DATABASE_NAME should have exactly 2 parts: <DATABASE_NAME>.<SCHEMA_NAME> gets %s', $inboundDatabaseName),
        );
        return $inboundDatabaseName;
    }
}
