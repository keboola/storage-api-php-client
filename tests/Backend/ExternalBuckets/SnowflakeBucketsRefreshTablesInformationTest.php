<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Utils\ConnectionUtils;

class SnowflakeBucketsRefreshTablesInformationTest extends BaseExternalBuckets
{
    use ConnectionUtils;

    public function testRefreshTablesInformationEndpointExists(): void
    {
        $bucketId = $this->initEmptyBucketWithDescription(self::STAGE_IN);
        $bucket = $this->_client->getBucket($bucketId);

        $bClient = $this->getBranchAwareDefaultClient($bucket['idBranch']);
        $bClient->refreshTableInformationInBucket($bucketId);
        $this->assertTrue(true, 'Testing only if requests working.');

        $this->dropBucketIfExists($this->_client, $bucketId);
    }

    public function testRefreshTablesInformation(): void
    {
        $createdTableRows = 10;

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace(
            [
                'backend' => self::BACKEND_SNOWFLAKE,
            ],
        );

        $bucketId = $this->initEmptyBucketWithDescription(self::STAGE_IN);
        $bucket = $this->_client->getBucket($bucketId);

        $tableId = $this->createTableWithRandomData('refresh-tables-information-test-table', $createdTableRows, 2);
        $table = $this->_client->getTable($tableId);

        $rowsCount = $table['rowsCount'];
        $dataSizeBytes = $table['dataSizeBytes'];

        $this->assertEquals($createdTableRows, $rowsCount);

        $db = $this->ensureSnowflakeConnection();

        $db->executeQuery(
            sprintf(
                'USE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier($workspace['connection']['database']),
            ),
        );

        $db->executeQuery(
            sprintf(
                'USE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier($bucketId),
            ),
        );

        $db->executeQuery(
            sprintf(
                'INSERT INTO %s VALUES (%s, %s)',
                SnowflakeQuote::quoteSingleIdentifier($table['name']),
                'testvalue 1',
                'testvalue 2',
            ),
        );

        $bClient = $this->getBranchAwareDefaultClient($bucket['idBranch']);

        $jobInfo = $bClient->refreshTableInformationInBucket($bucketId);

        $jobStatus = $bClient->waitForJob($jobInfo['id']);

        $this->assertNotNull($jobStatus);
        $this->assertEquals('success', $jobStatus['status']);

        $refreshedTable = $this->_client->getTable($tableId);

        $this->assertEquals($rowsCount + 1, $refreshedTable['rowsCount']);
        $this->assertNotEquals($dataSizeBytes, $refreshedTable['dataSizeBytes']);

        $this->_client->dropTable($tableId);
        $this->_client->dropBucket($bucketId);
        $workspaces->deleteWorkspace($workspace['id']);
    }
}
