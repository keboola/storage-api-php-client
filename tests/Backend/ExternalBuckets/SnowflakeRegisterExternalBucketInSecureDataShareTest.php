<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\ConnectionUtils;

class SnowflakeRegisterExternalBucketInSecureDataShareTest extends StorageApiTestCase
{
    use ConnectionUtils;

    public function setUp(): void
    {
        parent::setUp();
        $this->allowTestForBackendsOnly([self::BACKEND_SNOWFLAKE], 'Backend has to support external buckets');
    }

    public function testRegisterExternalBucket(): void
    {
        $externalTableNames = [
            'NAMES_TABLE',
            'SECURED_NAMES',
        ];

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
            null,
            true,
        );

        $registeredBucket = $this->_client->getBucket($bucketId);

        $this->assertSame($testBucketName, $registeredBucket['name']);
        $this->assertSame(self::STAGE_IN, $registeredBucket['stage']);
        $this->assertSame(
            sprintf('%s.%s', $projectRole, 'SDS_'.mb_strtoupper(str_replace('-', '_', $testBucketName))),
            $registeredBucket['path'],
        );

        $registeredTableNames = [];
        foreach ($registeredBucket['tables'] as $table) {
            $registeredTableNames[] = $table['name'];
        }

        $this->assertEquals($externalTableNames, $registeredTableNames, 'Not all external tables/views have registered view.');

        $this->_client->dropBucket($bucketId);
    }

    public function testInvalidDbToRegister(): void
    {
        $bucketName = 'test-sds-bucket';
        $bucketId = self::STAGE_IN.'.'.$bucketName;

        $this->dropBucketIfExists($this->_client, $bucketId);

        try {
            $this->_client->registerBucket(
                $bucketName,
                ['non-existing-database', 'non-existing-schema'],
                self::STAGE_IN,
                'will fail',
                'snowflake',
                'test-bucket-will-fail',
                true,
            );
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame('storage.dbObjectNotFound', $e->getStringCode(), $e->getMessage());
            $this->assertStringContainsString(
                'doesn\'t exist or project user is missing privileges to read from it.',
                $e->getMessage(),
            );
        }
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
}
