<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\ConnectionUtils;

class SnowflakeRegisterExternalBucketInSecureDataShareTest extends StorageApiTestCase
{
    use ConnectionUtils;

    public function testRegisterExternalBucket(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace0 = $workspaces->createWorkspace(['backend' => 'snowflake']);
        $projectRole = $workspace0['connection']['database'];

        $this->grantImportedPrivilegesToProjectRole($projectRole);

        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        if ($this->_client->bucketExists($bucketId)) {
            $this->_client->dropBucket($bucketId);
        }

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

        $this->_client->dropBucket($bucketId);
    }

    private function getInboundSharedDatabaseName(): string
    {
        $inboundDatabaseName = getenv('SNOWFLAKE_INBOUND_DATABASE_NAME');
        assert($inboundDatabaseName !== false, 'SNOWFLAKE_INBOUND_DATABASE_NAME env var is not set');
        $this->assertCount(2, explode('.', $inboundDatabaseName), 'SNOWFLAKE_INBOUND_DATABASE_NAME should have exactly 2 parts: <DATABASE_NAME>.<SCHEMA_NAME>');
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
